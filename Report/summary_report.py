# Databricks notebook source
"""
프랜차이즈 2차 AI 요약 보고서 파이프라인
---------------------------------------
- 1차 지표분석(JSON) + 부동산원 임대료 정보를 결합하여 요약 보고서(JSON) 생성
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import json

def extract_first_json_block(s: str) -> Optional[str]:
    stack, start = 0, None
    for i, ch in enumerate(s):
        if ch == '{':
            if stack == 0:
                start = i
            stack += 1
        elif ch == '}':
            stack -= 1
            if stack == 0 and start is not None:
                return s[start:i+1]
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
    raise ValueError("요약 JSON 파싱 실패")



@dataclass
class SummaryPipelineConfig:
    summary_prompt_uri: str                 # 요약 프롬프트 URI
    summary_model: str = "gpt-5-mini"       # 요약 모델명
    max_output_tokens: int = 7000           # 최대 토큰


class SummaryInputsMissing(RuntimeError):
    """요약에 필요한 선행 결과나 입력이 부족할 때 사용."""
    pass


class RealEstateRepository:
    """부동산원 임대료 테이블 조회 + 계층 JSON 생성."""
    def __init__(self, spark_session, rent_table: str = "gold.realestate.market_rent"):
        self.spark = spark_session
        self.table = rent_table

    # ---- 문자열 정규화 ----
    @staticmethod
    def _normalize_market_name(name: str) -> str:
        import re as _re
        return _re.sub(r'^\s*\d+\.\s*', '', name or '').strip()

    @staticmethod
    def _pretty_floor(name: str) -> str:
        import re as _re
        s = (name or '').strip()
        if s == '-1층':
            return '지하1층'
        if _re.fullmatch(r'\d+층', s):
            return f'지상{s}'
        return s

    # ---- 면적 구간 파싱 ----
    @staticmethod
    def _parse_range_key_ext(k: str):
        import re as _re
        label = k or ""
        nums = _re.findall(r'([0-9]+(?:\.[0-9]+)?)', label)
        lo = float(nums[0]) if len(nums) >= 1 else None
        hi = float(nums[1]) if len(nums) >= 2 else None
        return {"lo": lo, "hi": hi, "label": label}

    # ---- 지역 행 조회 ----
    def fetch_region_row(self, sido: str, sigungu: str) -> dict:
        from pyspark.sql import functions as F
        rent_df = self.spark.table(self.table)
        rows = (
            rent_df.filter((F.col("시도") == F.lit(sido)) & (F.col("시군구") == F.lit(sigungu)))
                   .limit(1)
                   .collect()
        )
        if not rows:
            raise ValueError(f"임대료 테이블에서 지역 '{sido} {sigungu}'를 찾지 못했습니다.")
        return rows[0].asDict(recursive=True)

    # ---- 임대 정보 블록 생성 ----
    @staticmethod
    def _won(x):
        return int(round(float(x))) if x is not None else None

    def _rent_block_from_per_m2(self, per_m2_low, per_m2_high, per_m2_avg, lo_m2, hi_m2):
        """하한×하한면적, 상한×상한면적, 평균×대표면적을 활용해 월세/보증금 구간 및 대표값 계산."""
        if (per_m2_low is None and per_m2_high is None and per_m2_avg is None) or (lo_m2 is None and hi_m2 is None):
            return None
        min_m2 = lo_m2 if lo_m2 is not None else hi_m2
        max_m2 = hi_m2 if hi_m2 is not None else lo_m2
        mid_m2 = ((lo_m2 + hi_m2) / 2.0) if (lo_m2 is not None and hi_m2 is not None) else (lo_m2 if lo_m2 is not None else hi_m2)
        min_rent = self._won(per_m2_low  * min_m2) if (per_m2_low  is not None and min_m2 is not None) else None
        max_rent = self._won(per_m2_high * max_m2) if (per_m2_high is not None and max_m2 is not None) else None
        rep_rent = self._won(per_m2_avg  * mid_m2) if (per_m2_avg  is not None and mid_m2 is not None) else None
        min_dep = self._won(min_rent * 10) if min_rent is not None else None
        max_dep = self._won(max_rent * 10) if max_rent is not None else None
        rep_dep = self._won(rep_rent * 10) if rep_rent is not None else None
        return {
            "제곱미터당_월세": {"하한": per_m2_low, "평균": per_m2_avg, "상한": per_m2_high},
            "면적_범위_m2": [min_m2, max_m2],
            "월임대료_범위": [min_rent, max_rent],
            "월임대료_대표": rep_rent,
            "임대보증금_범위": [min_dep, max_dep],
            "임대보증금_대표": rep_dep,
        }

    # ---- 상권×표본/층 임대료 계층 JSON 생성 ----
    def build_rent_hierarchy(self, region_dict: dict, area_bucket: dict) -> list:
        S_avg  = region_dict.get("상권별_표본구분별_m2당임대료평균") or {}
        S_low  = region_dict.get("상권별_표본구분별_m2당임대료하한") or {}
        S_high = region_dict.get("상권별_표본구분별_m2당임대료상한") or {}
        F_avg  = region_dict.get("상권별_조사층별_m2당임대료평균") or {}
        F_low  = region_dict.get("상권별_조사층별_m2당임대료하한") or {}
        F_high = region_dict.get("상권별_조사층별_m2당임대료상한") or {}

        # 면적 버킷 정렬 및 소/중/대 라벨
        area_items_ext = []
        if area_bucket:
            iterator = area_bucket.items() if hasattr(area_bucket, "items") else dict(area_bucket).items()
            parsed = []
            for k, cnt in iterator:
                info = self._parse_range_key_ext(k)
                if info["lo"] is None and info["hi"] is None:
                    continue
                mid = ((info["lo"] + info["hi"]) / 2.0) if (info["lo"] is not None and info["hi"] is not None) else (info["lo"] if info["lo"] is not None else info["hi"])
                parsed.append({
                    "면적구간": info["label"],
                    "하한": info["lo"], "상한": info["hi"],
                    "대표면적_m2": round(mid, 2) if mid is not None else None,
                    "사업장수": int(cnt) if cnt is not None else 0,
                })
            parsed.sort(key=lambda x: (x["대표면적_m2"] if x["대표면적_m2"] is not None else float("inf")))
            labels = ["소규모", "중규모", "대규모"]
            for i, it in enumerate(parsed[:3]):
                it["구분"] = labels[i] if i < len(labels) else f"규모{i+1}"
                area_items_ext.append(it)

        rent_hier = []
        for a in area_items_ext:
            lo, hi = a["하한"], a["상한"]
            size_block = {
                "구분": a["구분"],
                "면적구간": a["면적구간"],
                "대표면적_m2": a["대표면적_m2"],
                "사업장수": a["사업장수"],
                "표본구분_상권별": [],
                "조사층_상권별": [],
            }
            # 상권 × 표본구분
            for market_name_raw, inner_avg in (S_avg or {}).items():
                market_name = self._normalize_market_name(market_name_raw)
                low_map  = S_low.get(market_name_raw, {})  if isinstance(S_low, dict)  else {}
                high_map = S_high.get(market_name_raw, {}) if isinstance(S_high, dict) else {}
                for sample_name, avg_val in (inner_avg or {}).items():
                    block = self._rent_block_from_per_m2(
                        (low_map.get(sample_name)  if isinstance(low_map, dict)  else None),
                        (high_map.get(sample_name) if isinstance(high_map, dict) else None),
                        avg_val, lo, hi,
                    )
                    if block:
                        size_block["표본구분_상권별"].append({
                            "상권": market_name,
                            "표본구분": sample_name,
                            **block,
                        })
            # 상권 × 조사층
            for market_name_raw, inner_avg in (F_avg or {}).items():
                market_name = self._normalize_market_name(market_name_raw)
                low_map  = F_low.get(market_name_raw, {})  if isinstance(F_low, dict)  else {}
                high_map = F_high.get(market_name_raw, {}) if isinstance(F_high, dict) else {}
                for floor_name_raw, avg_val in (inner_avg or {}).items():
                    block = self._rent_block_from_per_m2(
                        (low_map.get(floor_name_raw)  if isinstance(low_map, dict)  else None),
                        (high_map.get(floor_name_raw) if isinstance(high_map, dict) else None),
                        avg_val, lo, hi,
                    )
                    if block:
                        size_block["조사층_상권별"].append({
                            "상권": market_name,
                            "조사층": self._pretty_floor(floor_name_raw),
                            **block,
                        })
            size_block["표본구분_상권별"].sort(key=lambda x: (x.get("월임대료_대표") is None, x.get("월임대료_대표")))
            size_block["조사층_상권별"].sort(key=lambda x: (x.get("월임대료_대표") is None, x.get("월임대료_대표")))
            rent_hier.append(size_block)
        return rent_hier


# =====================================
# LLM/프롬프트 인터페이스 (1차 파이프라인과 동일 규격 가정)
# =====================================

class PromptLoader:
    def load(self, uri: str):
        raise NotImplementedError


class LLMClient:
    def generate(self, *, model: str, system: str, user: str, max_output_tokens: int) -> str:
        raise NotImplementedError


# =====================================
# 요약 오케스트레이터
# =====================================

class SummaryReportPipeline:
    """1차 지표분석 결과 + 부동산 임대료 계층을 기반으로 2차 요약 JSON 생성 및 저장."""

    def __init__(self,
                 cfg: SummaryPipelineConfig,
                 realestate_repo: RealEstateRepository,
                 prompt_loader: PromptLoader,
                 llm: LLMClient):
        self.cfg = cfg
        self.re_repo = realestate_repo
        self.prompt_loader = prompt_loader
        self.llm = llm

    # ---- 실행 ----
    def run(self, *,
            user,
            target_brand_number: str,
            target_brand_name: str,
            brand_row_context_json: str,
            area_bucket: Optional[dict],
            metric_analysis_json_obj: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        # sido/sigungu를 그대로 사용 (정규화/파싱 없음)
        sido = getattr(user, "sido", "") if not isinstance(user, dict) else user.get("sido", "")
        sigungu = getattr(user, "sigungu", "") if not isinstance(user, dict) else user.get("sigungu", "")

        # 지역 임대료 로드
        region_dict = self.re_repo.fetch_region_row(sido, sigungu)
        rent_hier = self.re_repo.build_rent_hierarchy(region_dict, area_bucket or {})
        rent_hier_json = json.dumps(rent_hier, ensure_ascii=False)

        # 1차 분석 JSON (반드시 호출 인자로 전달되어야 함)
        if metric_analysis_json_obj is None:
            raise SummaryInputsMissing("metric_analysis_json_obj 가 필요합니다. 1차 지표분석 JSON을 인자로 전달하세요.")
        metric_analysis_json = json.dumps(metric_analysis_json_obj, ensure_ascii=False)

        # 프롬프트
        bundle = self.prompt_loader.load(self.cfg.summary_prompt_uri)
        system_prompt = getattr(bundle, 'system', '')
        user_template = getattr(bundle, 'user_template', '')
        if not user_template:
            raise ValueError("프롬프트 템플릿이 비어 있습니다.")

        user_filled = user_template.format(
            # ✅ region을 만들지 않고, sido/sigungu를 그대로 보냄
            sido=sido,
            sigungu=sigungu,
            industry=getattr(user, "industry", "") if not isinstance(user, dict) else user.get("industry", ""),
            budget=getattr(user, "budget_excl_rent", "") if not isinstance(user, dict) else user.get("budget_excl_rent", ""),
            needs=getattr(user, "needs", "") if not isinstance(user, dict) else user.get("needs", ""),
            brand_no=target_brand_number,
            brand_name=target_brand_name,
            metric_analysis_json=metric_analysis_json,
            context_json=brand_row_context_json,
            rent_hier_json=rent_hier_json,
        )

        raw = self.llm.generate(
            model=self.cfg.summary_model,
            system=system_prompt,
            user=user_filled,
            max_output_tokens=self.cfg.max_output_tokens,
        )
        summary_obj = parse_json_strict(raw)
        return summary_obj


    # ---- MLflow 저장  ----
    def log_summary_to_mlflow(self, summary_obj: Dict[str, Any], *, brand_no: str, brand_name: str) -> None:
        try:
            import mlflow
            with mlflow.start_run(nested=True):
                mlflow.set_tags({
                    "stage": "summary_v3",
                    "brand_no": brand_no,
                    "brand_name": brand_name,
                    "prompt_uri": self.cfg.summary_prompt_uri,
                })
                mlflow.log_param("model", self.cfg.summary_model)
                mlflow.log_dict(summary_obj, f"ai_summary_{brand_no}_{brand_name}.json")
        except Exception as e:
            print(f"[warn] MLflow 요약 저장 실패: {e}")


if __name__ == "__main__":
    print("이 모듈은 Databricks 노트북 또는 드라이버 스크립트에서 임포트하여 사용하세요.")

