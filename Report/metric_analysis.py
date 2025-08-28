# Databricks notebook source
# Report/metric_analysis.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, Tuple
import json, re


# ------------------------------
# DTO (UserInput 제거)
# ------------------------------
@dataclass
class PromptBundle:
    system: str
    user_template: str

@dataclass
class PipelineConfig:
    table_name: str
    industry_summary_table: str
    prompt_uri: str
    model: str
    max_output_tokens: int = 8000
    value_cutoff: int = 300

@dataclass
class PipelineResult:
    user_input: Dict[str, Any]
    target_brand_number: str
    target_brand_name: str
    metric_json: Dict[str, str]
    raw_model_output: str

# ------------------------------
# 인터페이스 (Protocol)
# ------------------------------
class FranchiseRepository(Protocol):
    def fetch_brand_row(self, table_name: str, brand_number: str) -> Tuple[Dict[str, Any], str]: ...
    def fetch_industry_row(self, industry_summary_table: str, industry: str) -> Optional[Dict[str, Any]]: ...

class PromptLoader(Protocol):
    def load(self, uri: str) -> PromptBundle: ...

class LLMClient(Protocol):
    def generate(self, *, model: str, system: str, user: str, max_output_tokens: int) -> str: ...

# ------------------------------
# Spark 기반 Repository
# ------------------------------
class SparkFranchiseRepository:
    def __init__(self, spark_session):
        self.spark = spark_session

    @staticmethod
    def _row_to_context_dict(row_obj, max_len: int = 300) -> Dict[str, str]:
        d: Dict[str, str] = {}
        for k, v in row_obj.asDict(recursive=True).items():
            if v is None:
                continue
            s = str(v)
            if len(s) > max_len:
                s = s[:max_len] + "…"
            d[k] = s
        return d

    def fetch_brand_row(self, table_name: str, brand_number: str) -> Tuple[Dict[str, Any], str]:
        from pyspark.sql import functions as F
        df = self.spark.table(table_name)
        brand_df = df.filter(F.col("공정위_등록번호") == brand_number).limit(1)
        rows = brand_df.collect()
        if not rows:
            raise ValueError(f"테이블에서 '{brand_number}' 행을 찾지 못했습니다.")
        row = rows[0]
        brand_name = row["공정위_영업표지_브랜드명"]
        return self._row_to_context_dict(row), brand_name

    def fetch_industry_row(self, industry_summary_table: str, industry: str) -> Optional[Dict[str, Any]]:
        from pyspark.sql import functions as F
        sdf = self.spark.table(industry_summary_table)
        rows = (
            sdf.filter(F.col("공정위_업종분류") == industry)
               .limit(1)
               .collect()
        )
        if rows:
            return self._row_to_context_dict(rows[0])
        return None

# ------------------------------
# 프롬프트 로더 (mgenai)
# ------------------------------
class MgenaiPromptLoader:
    def __init__(self, mgenai_module):
        self.mgenai = mgenai_module

    def load(self, uri: str) -> PromptBundle:
        p = self.mgenai.load_prompt(uri)
        tmpl = getattr(p, "template", p)
        system_prompt = ""
        user_template = ""
        if isinstance(tmpl, dict):
            system_prompt = tmpl.get("system", "")
            user_template = tmpl.get("user", "") or tmpl.get("template", "")
        elif isinstance(tmpl, str):
            try:
                j = json.loads(tmpl)
                if isinstance(j, dict):
                    system_prompt = j.get("system", "")
                    user_template = j.get("user", "") or j.get("template", "")
                else:
                    user_template = str(j)
            except Exception:
                user_template = tmpl
        else:
            user_template = str(tmpl)
        if not user_template:
            raise ValueError("프롬프트 템플릿을 찾지 못했습니다.")
        return PromptBundle(system=system_prompt, user_template=user_template)

# ------------------------------
# LLM Client (OpenAI Responses API)
# ------------------------------
class OpenAIResponsesClient:
    def __init__(self, openai_client):
        self.client = openai_client

    def generate(self, *, model: str, system: str, user: str, max_output_tokens: int) -> str:
        resp = self.client.responses.create(
            model=model,
            instructions=system,
            input=user,
            reasoning={"effort": "minimal"},
            text={"verbosity": "medium"},
            max_output_tokens=max_output_tokens,
        )
        raw_text = (getattr(resp, "output_text", "") or "").strip()
        if raw_text:
            return raw_text
        parts = []
        for item in getattr(resp, "output", []) or []:
            for c in getattr(item, "content", []) or []:
                tv = getattr(getattr(c, "text", None), "value", None)
                if tv:
                    parts.append(str(tv))
        raw_text = "\n".join(parts).strip()
        if not raw_text:
            raise RuntimeError("모델 응답이 비어 있습니다.")
        return raw_text

# ------------------------------
# JSON 파싱 / 정제 유틸
# ------------------------------
_JSON_BLOCK_RE = re.compile(r"\{(?:[^{}]|\{[^{}]*\})*\}")

def extract_first_json_block(s: str) -> Optional[str]:
    m = _JSON_BLOCK_RE.search(s)
    if m:
        return m.group(0)
    stack, start = 0, None
    for i, ch in enumerate(s):
        if ch == '{':
            if stack == 0:
                start = i
            stack += 1
        elif ch == '}':
            stack -= 1
            if stack == 0 and start is not None:
                return s[start:i + 1]
    return None

def parse_json_strict(s: str) -> Dict[str, Any]:
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    block = extract_first_json_block(s)
    if block:
        return json.loads(block)
    raise ValueError("유효한 JSON을 파싱하지 못했습니다.")

def clean_value(v: Any) -> str:
    if not isinstance(v, str):
        return "" if v is None else str(v)
    s = v.replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())
    return s

# ------------------------------
# 내부 헬퍼: dict/객체 모두 지원
# ------------------------------
def _u(user: Any, key: str) -> str:
    if isinstance(user, dict):
        return user.get(key, "")
    return getattr(user, key, "")

# ------------------------------
# 오케스트레이터
# ------------------------------
class MetricAnalysisPipeline:
    def __init__(self, cfg, repo, prompt_loader, llm):
        self.cfg = cfg; self.repo = repo; self.prompt_loader = prompt_loader; self.llm = llm

    def _build_user_prompt(self, bundle, brand_ctx, brand_name, industry_ctx, user):
        context_json = json.dumps(brand_ctx, ensure_ascii=False)
        industry_json = json.dumps(industry_ctx or {}, ensure_ascii=False)
        return bundle.user_template.format(
            sido=_u(user, "sido"),
            sigungu=_u(user, "sigungu"),
            industry=_u(user, "industry"),
            budget=_u(user, "budget_excl_rent"),
            needs=_u(user, "needs"),
            brand=brand_name,
            context_json=context_json,
            industry_summary_json=industry_json
        )



    def _filter_allowed(self, analysis_json: Dict[str, Any], cutoff: int) -> Dict[str, str]:
        out = {}
        for k, v in analysis_json.items():
            s = "" if v is None else " ".join(str(v).replace("\r"," ").replace("\n"," ").split())
            out[k] = (s[:cutoff] + "…") if len(s) > cutoff else s
        return out

    def run(self, user: Any, *, target_brand_number: str) -> PipelineResult:
        brand_ctx, brand_name = self.repo.fetch_brand_row(self.cfg.table_name, target_brand_number)
        industry_ctx = self.repo.fetch_industry_row(self.cfg.industry_summary_table, _u(user,"industry"))
        bundle = self.prompt_loader.load(self.cfg.prompt_uri)
        user_prompt = self._build_user_prompt(bundle, brand_ctx, brand_name, industry_ctx, user)

        raw = self.llm.generate(
            model=self.cfg.model, system=bundle.system, user=user_prompt,
            max_output_tokens=self.cfg.max_output_tokens
        )
        analysis_json = parse_json_strict(raw)
        metric_json = self._filter_allowed(analysis_json, self.cfg.value_cutoff)

        return PipelineResult(
            user_input={
                "시도": _u(user, "sido"),
                "시군구": _u(user, "sigungu"),
                "업종": _u(user,"industry"),
                "임대료_및_임대보증금_제외_창업비용": _u(user,"budget_excl_rent"),
                "니즈사항": _u(user,"needs"),
            },
            target_brand_number=target_brand_number,
            target_brand_name=brand_name,
            metric_json=metric_json,
            raw_model_output=raw
        )

    def log_result_to_mlflow(self, result: PipelineResult, run_name: Optional[str] = None) -> None:
        try:
            import mlflow
            fname = f"metric_analysis_{result.target_brand_number}_{result.target_brand_name}.json"
            with mlflow.start_run(run_name=run_name, nested=True):
                mlflow.log_dict({
                    "사용자입력": result.user_input,
                    "추천브랜드_공정위등록번호": result.target_brand_number,
                    "추천브랜드명": result.target_brand_name,
                    "지표분석_JSON": result.metric_json,
                    "모델원본응답": result.raw_model_output,
                }, fname)
        except Exception as e:
            print(f"[warn] MLflow 로깅 실패: {e}")

# ------------------------------
# LangChain / LangGraph 연동용 어댑터
# ------------------------------
class LangChainRunnable:
    def __init__(self, pipeline: MetricAnalysisPipeline):
        self.pipeline = pipeline

    def __call__(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        user = inputs.get("user", inputs) if isinstance(inputs, dict) else {}
        target_brand_number = inputs.get("target_brand_number") or (user.get("target_brand_number") if isinstance(user, dict) else getattr(user, "target_brand_number", None))
        if not target_brand_number:
            raise ValueError("target_brand_number 가 필요합니다.")
        r = self.pipeline.run(user, target_brand_number=target_brand_number)
        return {
            "user_input": r.user_input,
            "target_brand_number": r.target_brand_number,
            "target_brand_name": r.target_brand_name,
            "metric_json": r.metric_json,
            "raw_model_output": r.raw_model_output,
        }

# ------------------------------
# 파이프라인 생성 편의 함수
# ------------------------------
def make_default_pipeline(spark, mgenai, openai_client, *,
                          table_name: str,
                          industry_summary_table: str,
                          prompt_uri: str,
                          model: str = "gpt-5-mini",
                          max_output_tokens: int = 8000) -> MetricAnalysisPipeline:
    cfg = PipelineConfig(
        table_name=table_name,
        industry_summary_table=industry_summary_table,
        prompt_uri=prompt_uri,
        model=model,
        max_output_tokens=max_output_tokens
    )
    repo = SparkFranchiseRepository(spark)
    loader = MgenaiPromptLoader(mgenai)
    llm = OpenAIResponsesClient(openai_client)
    return MetricAnalysisPipeline(cfg, repo, loader, llm)

