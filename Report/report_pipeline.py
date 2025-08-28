# Databricks notebook source
# Report/report_pipeline.py
from __future__ import annotations
import json
from typing import Any, Dict

from pyspark.sql import functions as F
from langchain_core.runnables import RunnableLambda, RunnableMap

from Report.metric_analysis import (
    make_default_pipeline, LangChainRunnable,
    SparkFranchiseRepository, MgenaiPromptLoader, OpenAIResponsesClient
)
from Report.summary_report import (
    SummaryPipelineConfig, RealEstateRepository, SummaryReportPipeline
)

def run_report_pipeline(
    *,
    spark,
    mgenai_module,
    openai_client,
    user: Dict[str, Any] | Any,   # dict 또는 속성객체 모두 OK
    brand_no: str,
    model: str = "gpt-5-mini",

    # 데이터/프롬프트 기본값
    table_name: str = "gold.master.franchise_mois",
    industry_summary_table: str = "gold.master.franchise_industry_summary",
    rent_table: str = "gold.realestate.market_rent",
    metric_prompt_uri: str = "prompts:/gold.default.franchise_metricanalysis_prompt/9",
    summary_prompt_uri: str = "prompts:/gold.default.franchise_summarizing_prompt/9",
) -> Dict[str, Any]:
    """
    사용자 입력과 브랜드번호를 받아:
      1) 1차 지표분석(LCEL) → metric_json
      2) 2차 요약(LCEL) → summary_json
    을 생성하여 반환합니다.
    반환: {"brand_no","brand_name","metric","summary"}
    """

    # --- 1) 1차 파이프라인 준비
    metric_pipeline = make_default_pipeline(
        spark, mgenai_module, openai_client,
        table_name=table_name,
        industry_summary_table=industry_summary_table,
        prompt_uri=metric_prompt_uri,
        model=model,
        max_output_tokens=8000,
    )
    metric_runnable = LangChainRunnable(metric_pipeline)

    # --- 2) 2차 파이프라인 준비
    summary_cfg = SummaryPipelineConfig(
        summary_prompt_uri=summary_prompt_uri,
        summary_model=model,
        max_output_tokens=8000,
    )
    re_repo = RealEstateRepository(spark, rent_table=rent_table)
    summ_pipeline = SummaryReportPipeline(
        summary_cfg, re_repo, MgenaiPromptLoader(mgenai_module), OpenAIResponsesClient(openai_client)
    )

    # --- 3) 2차 실행 함수 (LCEL의 마지막 단계)
    def _run_summary(inputs: Dict[str, Any]) -> Dict[str, Any]:
        metric = inputs["metric"]
        u = inputs["user"]  # dict/속성 모두 허용
        target_brand_no = metric["target_brand_number"]
        target_brand_name = metric["target_brand_name"]

        # 원본 row 재조회 → area_bucket & 300자 컷 컨텍스트 생성
        row = (spark.table(table_name)
                   .filter(F.col("공정위_등록번호") == target_brand_no)
                   .limit(1).collect()[0])
        area_bucket = row.asDict(recursive=True).get("면적구간별_사업장수_map")
        brand_row_context_json = json.dumps(
            SparkFranchiseRepository._row_to_context_dict(row), ensure_ascii=False
        )

        # 2차 실행 (1차 결과 + 사용자 입력 전달)
        summary_obj = summ_pipeline.run(
            user=u,
            target_brand_number=target_brand_no,
            target_brand_name=target_brand_name,
            brand_row_context_json=brand_row_context_json,
            area_bucket=area_bucket,
            metric_analysis_json_obj=metric["metric_json"],
        )
        return {"summary_json": summary_obj, "metric": metric}

    # --- 4) LCEL 체인 구성: user → metric → summary
    chain = (
        RunnableMap({
            "user":   RunnableLambda(lambda _: user),
            "metric": (RunnableLambda(lambda _: {"user": user, "target_brand_number": brand_no}) | metric_runnable)
        })
        | RunnableLambda(_run_summary)
    )

    out = chain.invoke({})
    metric = out["metric"]
    summary = out["summary_json"]
    brand_name = metric["target_brand_name"]

    return {
        "brand_no": brand_no,
        "brand_name": brand_name,
        "metric": metric,          # 1차 결과 dict (metric_json 포함)
        "summary": summary,        # 2차 요약 JSON
    }

