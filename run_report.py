# Databricks notebook source
# run_report.py

from __future__ import annotations
import os, json
from types import SimpleNamespace
from openai import OpenAI
from pyspark.sql import SparkSession
import mlflow, mlflow.genai as mgenai

# AI 요약보고서 생성 (1차 지표분석 -> 2차 AI 요약보고서 생성 파이프라인)
from Report.report_pipeline import run_report_pipeline

# 브랜드 최신 뉴스 요약
from Report.news_summary import run_news_summary

# RAG 기반 추천 브랜드 선정
try:
    from Recommendation.rag_brandRecommendation import top3_brand_numbers
except Exception:
    def top3_brand_numbers(_):
        # TODO: RAG 완성되면 제거
        return ["20180070", "20080100014"]


def get_openai_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY")
    return OpenAI(api_key=api_key)

def main():
    spark = SparkSession.builder.getOrCreate()
    client = get_openai_client()
    MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")

    # 사용자 입력은 run_report에서만 정의 (sido/sigungu 사용)
    user = {
        "sido": "경기",
        "sigungu": "용인시 기흥구",
        "industry": "커피",
        "budget_excl_rent": "5000만원~6000만원",
        "needs": "매달 지출되는 돈이 적으면 좋을 것 같고, 수익이 꾸준하면 좋을 것 같아요."
    }
    # RAG에서 속성 접근을 할 수 있도록 Namespace도 만들어 전달
    user_ns = SimpleNamespace(**user)

    # Naver API 키 (있으면 뉴스요약 수행)
    NAVER_ID = os.environ.get("NAVER_CLIENT_ID", "")
    NAVER_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")

    results = []
    with mlflow.start_run(run_name="franchise_full_chain"):
        # 1) RAG Top-3
        top3 = top3_brand_numbers(user_ns)

        # 2) 각 브랜드별: report_pipeline(1차+2차) → news_summary
        for brand_no in top3:
            # --- report_pipeline: 1차→2차 (LCEL 내부 연결)
            report_out = run_report_pipeline(
                spark=spark,
                mgenai_module=mgenai,
                openai_client=client,
                user=user,
                brand_no=brand_no,
                model=MODEL,
                table_name="gold.master.franchise_mois",
                industry_summary_table="gold.master.franchise_industry_summary",
                rent_table="gold.realestate.market_rent",
                metric_prompt_uri="prompts:/gold.default.franchise_metricanalysis_prompt/8",
                summary_prompt_uri="prompts:/gold.default.franchise_summarizing_prompt/8",
            )

            # --- news_summary: 뉴스요약 (키 있으면 수행)
            news_obj = {}
            if NAVER_ID and NAVER_SECRET:
                news_obj = run_news_summary(
                    brand_no=brand_no, spark=spark, openai_client=client, mgenai_module=mgenai,
                    naver_client_id=NAVER_ID, naver_client_secret=NAVER_SECRET,
                    per_brand_max_items=20, days=14, per_page=20, pages=4,
                    prompt_uri="prompts:/gold.default.franchise_news_summarizing_prompt/4",
                    openai_model=MODEL
                )

            # --- MLflow 저장 (인덱스 및 산출물)
            metric   = report_out["metric"]
            summary  = report_out["summary"]
            bname    = report_out["brand_name"]

            mlflow.log_dict(metric,  f"metric_stage1_{brand_no}_{bname}.json")
            mlflow.log_dict(summary, f"summary_{brand_no}_{bname}.json")
            if news_obj:
                mlflow.log_dict(news_obj, f"news_{brand_no}_{bname}.json")

            results.append({
                "brand_no": brand_no,
                "brand_name": bname,
                "metric_keys": list(metric["metric_json"].keys()),
                "summary_keys": list(summary.keys()),
                "news_keys": list(((news_obj.get("news_result") or {}) .keys()) if news_obj else []),
            })

        mlflow.log_dict({"index": results}, "full_chain_index.json")

if __name__ == "__main__":
    main()


