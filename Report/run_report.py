# Databricks notebook source
# run_pipeline.py
from __future__ import annotations

from typing import List, Dict, Any
import json
from pyspark.sql import functions as F

# LangChain runnables (버전 호환)
try:
    from langchain_core.runnables import RunnableLambda
except Exception:
    from langchain.schema.runnable import RunnableLambda  # 구버전 호환

# AI 요약 보고서 생성
from Report.metric_analysis import run_metric_analysis       # metric_summary
from Report.report_summary import run_summary_report         # report_summary
from Report.news_summary import run_news_summary             # news_summary


# (임시) RAG 브랜드 추천
try:
    from Recommendation.rag_recommendation import run_rag_recommendation
except Exception:
    # 임포트 실패 시 폴백: 동일한 시그니처로 스텁 정의
    def run_rag_recommendation(
        *, 
        sido: str, 
        sigungu: str, 
        industry: str, 
        budget_excl_rent: str, 
        needs: str
    ) -> list[str]:
        """RAG 미구현/임포트 실패 시 기본 추천 두 개를 반환."""
        return ["20180070", "20080100014"]


# -------------------------------
# 파이프라인 본체
# -------------------------------
def run_pipeline() -> List[Dict[str, Any]]:
    # 1) (임시) 사용자 입력
    user_input = {
        "sido": "인천",
        "sigungu": "남동구",
        "industry": "커피",
        "budget_excl_rent": "5000만원~6000만원",
        "needs": "매달 지출되는 돈이 적으면 좋을 것 같고, 수익이 꾸준하면 좋을 것 같아요.",
    }

    # 2) RAG 추천
    rec_brand_nos = run_rag_recommendation(**user_input)

    # 3) 추천 브랜드명 일괄 조회
    mois_table = "gold.master.franchise_mois"
    df = spark.table(mois_table).select(
        "공정위_등록번호", "공정위_영업표지_브랜드명", "면적구간별_사업장수_map"
    )
    brand_rows = df.filter(F.col("공정위_등록번호").isin(rec_brand_nos)).collect()

    # brand_no -> (brand_name, brand_row_dict) 맵
    brand_lookup: Dict[str, Dict[str, Any]] = {}
    for r in brand_rows:
        d = r.asDict(recursive=True)
        brand_lookup[d["공정위_등록번호"]] = {
            "brand_name": d["공정위_영업표지_브랜드명"],
            "brand_row": d,  # summary에서 면적구간 맵 사용 가능
        }

    results: List[Dict[str, Any]] = []

    # 4) 각 브랜드별 실행
    for brand_no in rec_brand_nos:
        info = brand_lookup.get(brand_no)
        if not info:
            results.append({
                "brand_no": brand_no,
                "error": f"브랜드명을 테이블({mois_table})에서 찾지 못함",
            })
            continue

        brand_name = info["brand_name"]
        brand_row  = info["brand_row"]

        try:
            # metric -> summary 체인
            metric_chain = RunnableLambda(lambda _:
                run_metric_analysis(
                    sido=user_input["sido"],
                    sigungu=user_input["sigungu"],
                    industry=user_input["industry"],
                    budget_excl_rent=user_input["budget_excl_rent"],
                    needs=user_input["needs"],
                    brand_no=brand_no,
                    brand_name=brand_name,
                )
            )

            summary_chain = RunnableLambda(lambda metric_out:
                run_summary_report(
                    sido=user_input["sido"],
                    sigungu=user_input["sigungu"],
                    industry=user_input["industry"],
                    budget_excl_rent=user_input["budget_excl_rent"],
                    needs=user_input["needs"],
                    brand_no=brand_no,
                    brand_name=brand_name,
                    metric_analysis=metric_out.get("지표분석_JSON", {}) if isinstance(metric_out, dict) else {},
                    brand_row=brand_row,      # 면적구간 맵 활용
                    brand_context=brand_row,  # 요약 프롬프트 컨텍스트
                )
            )

            metric_result  = metric_chain.invoke(None)
            summary_result = summary_chain.invoke(metric_result)

            # 뉴스 요약
            news_result = run_news_summary(
                brand_no=brand_no,
                brand_name=brand_name,
            )

            results.append({
                "brand_no": brand_no,
                "brand_name": brand_name,
                "report_summary": summary_result,    # 최종 AI 요약보고서(JSON)
                "news_summary": news_result,         # 뉴스 선정/요약(JSON)
            })

        except Exception as e:
            results.append({
                "brand_no": brand_no,
                "brand_name": brand_name,
                "error": f"pipeline failed: {type(e).__name__}: {e}",
            })

    return results


# --- 예시 실행 ---
if __name__ == "__main__":
    out = run_pipeline()
    print(json.dumps(out, ensure_ascii=False, indent=2))

