# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Any, Dict, List

import time
from datetime import datetime


# =========================================
# .env 로드 (선택)
# =========================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# =========================================
# 모듈 임포트 (recommender: 같은 폴더 or Recommendation/ 하위)
# =========================================
import pathlib
BASE = pathlib.Path(__file__).resolve().parent
if (BASE / "recommender.py").exists():
    sys.path.insert(0, str(BASE))
elif (BASE / "Recommendation" / "recommender.py").exists():
    sys.path.insert(0, str(BASE))

try:
    # 같은 폴더
    from Recommendation.recommender import recommend_brand_ids
except ModuleNotFoundError:
    # Recommendation/ 패키지
    from Recommendation.recommender import recommend_brand_ids  # type: ignore

from Report.metric_analysis import run_metric_analysis
from Report.report_summary import run_summary_report
from Report.news_summary import run_news_summary

import numpy as np
from datetime import datetime, date
from decimal import Decimal

def _now():
    return datetime.now().strftime("%H:%M:%S")

def _elapsed_ms(ts_start):
    return int((time.time() - ts_start) * 1000)
# =========================================
# Databricks Serverless(SQL Warehouse) 연결
#  - databricks-sql-connector 사용 (Spark 미사용)
# =========================================
MOIS_TABLE = "gold.master.franchise_mois"
def to_json_safe(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, set):
        return list(o)
    if isinstance(o, dict):
        return {str(k): to_json_safe(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [to_json_safe(x) for x in o]
    return o
def fetch_brands_from_warehouse(brand_nos: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Serverless(SQL Warehouse)에서 브랜드 메타 정보 조회.
    - .env 필요 키:
        DATABRICKS_SERVER_HOSTNAME=dbc-xxxx.cloud.databricks.com  (https:// 제외)
        DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890
        DATABRICKS_TOKEN=dapiex-...
    - 연결 실패/미설정 시 빈 dict 반환 (파이프라인은 계속 진행)
    """
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not brand_nos:
        return {}
    if not (server_hostname and http_path and access_token):
        print("[INFO] Serverless 접속 정보가 없어 Warehouse 조회를 건너뜁니다.")
        return {}

    try:
        from databricks import sql as dbsql
    except Exception as e:
        print(f"[WARN] databricks-sql-connector 미설치 또는 임포트 실패: {e}")
        return {}

    placeholders = ",".join(["?"] * len(brand_nos))
    query = f"""
        SELECT *
        FROM {MOIS_TABLE}
        WHERE `공정위_등록번호` IN ({placeholders})
    """
    

    lookup: Dict[str, Dict[str, Any]] = {}
    try:
        with dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query, brand_nos)
                # 1) 컬럼명 확보
                colnames = [d[0] for d in cur.description]  # e.g. ['공정위_등록번호', '공정위_영업표지_브랜드명', '면적구간별_사업장수_map', ...]
                rows = cur.fetchall()
                # # 드라이버 버전에 따라 Row 또는 튜플
                # for row in rows:
                #     # 컬럼 순서는 SELECT 순서대로 사용
                #     brand_no = str(row[0])
                #     brand_name = row[1]
                #     area_map = row[2]
                #     lookup[brand_no] = {
                #         "brand_name": brand_name,
                #         "brand_row": {
                #             "공정위_등록번호": brand_no,
                #             "공정위_영업표지_브랜드명": brand_name,
                #             "면적구간별_사업장수_map": area_map,
                #         },
                #     }
                 # 2) 모든 컬럼을 dict로 변환
                for row in rows:
                    # databricks-sql-connector는 tuple(Row) 형태 → 컬럼명 zip
                    row_dict = {col: row[i] for i, col in enumerate(colnames)}
                    row_dict = to_json_safe(row_dict)
                    # 문자열 JSON/맵 컬럼들 파싱 시도(있을 때만)
                    for k, v in list(row_dict.items()):
                        if isinstance(v, str):
                            s = v.strip()
                            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                                try:
                                    row_dict[k] = json.loads(s)
                                except Exception:
                                    pass

                    brand_no = str(row_dict.get("공정위_등록번호") or "")
                    brand_nm = row_dict.get("공정위_영업표지_브랜드명") or f"브랜드({brand_no})"

                    # 🔴 여기서 '전체 행'을 그대로 brand_row로 넣어줍니다
                    lookup[brand_no] = {
                        "brand_name": brand_nm,
                        "brand_row": row_dict,   # ← 전체 컬럼 key/value
                    }
    except Exception as e:
        print(f"[WARN] Warehouse 조회 실패: {e}")
        return {}

    return lookup


# =========================================
# 기본 요청값 (네 엔진 인터페이스에 맞춤)
# =========================================
DEFAULT_REQUEST: Dict[str, Any] = {
    "regions": ["서울"],
    "sido": "서울",
    "sigungu": "마포구",
    "category": "한식",
    "budget_bucket": "3천만원~6천만원",
    "needs_query": "초보 창업자도 시작하기 좋은 브랜드",
    "top_k": 2,
    # 성능/튜닝 노브
    "multi_query_count": 3,
    "initial_search_limit": 50,
    "rerank_topk": 32,
    "pair_trunc": 512,
    "ce_batch": 32,
    "ce_device": None,  # 로컬 CPU면 None 또는 "cpu"
}


# =========================================
# 파이프라인
# =========================================
def run_pipeline(req: Dict[str, Any]) -> List[Dict[str, Any]]:
    t0 = time.time()
    print(f"[{_now()}] [PIPE] start | req={{regions:{req.get('regions')}, sido:{req.get('sido')}, sigungu:{req.get('sigungu')}, cat:{req.get('category')}, budget:{req.get('budget_bucket')}}}")

    # 1) 추천 (Qdrant + E5 + CE/BGE)
    t_rec = time.time()
    recs = recommend_brand_ids(
        regions=req.get("regions"),
        category=req.get("category"),
        budget_bucket=req.get("budget_bucket"),
        needs_query=req["needs_query"],
        top_k=int(req.get("top_k", 2)),
        multi_query_count=int(req.get("multi_query_count", 3)),
        initial_search_limit=int(req.get("initial_search_limit", 50)),
        rerank_topk=int(req.get("rerank_topk", 32)),
        pair_trunc=int(req.get("pair_trunc", 512)),
        ce_batch=int(req.get("ce_batch", 32)),
        ce_device=req.get("ce_device"),
    )
    rec_brand_nos = [r["brand_no"] for r in recs]
    print(f"[{_now()}] [PIPE] rec done | n={len(rec_brand_nos)} | elapsed={_elapsed_ms(t_rec)}ms | brands={rec_brand_nos}")
    if not rec_brand_nos:
        print(f"[{_now()}] [PIPE] empty rec result, abort.")
        return [{"error": "추천 결과가 비어 있습니다."}]

    # 2) Serverless(SQL Warehouse)에서 브랜드 메타 조회
    t_wh = time.time()
    brand_lookup = fetch_brands_from_warehouse(rec_brand_nos)
    print(f"[{_now()}] [PIPE] warehouse lookup done | hit={len(brand_lookup)} | miss={len(rec_brand_nos)-len(brand_lookup)} | elapsed={_elapsed_ms(t_wh)}ms")


    # 3) 못 찾은 브랜드는 최소정보로 보강
    miss = [bn for bn in rec_brand_nos if bn not in brand_lookup]
    if miss:
        print(f"[{_now()}] [PIPE] warehouse miss brands={miss}")
        for bn in miss:
            brand_lookup[bn] = {"brand_name": f"브랜드({bn})", "brand_row": {}}

    results: List[Dict[str, Any]] = []
    sido = req.get("sido")
    sigungu = req.get("sigungu")

    # 4) 브랜드별: 지표 → 요약 → 뉴스
    for idx, rec in enumerate(recs, start=1):
        brand_no = rec["brand_no"]
        rerank_score = rec.get("rerank_score")
        brand_info = brand_lookup.get(brand_no, {"brand_name": f"브랜드({brand_no})", "brand_row": {}})
        brand_name = brand_info.get("brand_name") or f"브랜드({brand_no})"
        brand_row = brand_info.get("brand_row") or {}

        print(f"[{_now()}] [B{idx}/{len(rec_brand_nos)}:{brand_no}] start | name={brand_name}")
        print(brand_row)
        # 4-1) 지표분석
        t_m = time.time()
        metric_out = run_metric_analysis(
            sido=sido,
            sigungu=sigungu,
            industry=req.get("category") or "",
            budget_excl_rent=req.get("budget_bucket") or "",
            needs=req.get("needs_query") or "",
            brand_no=brand_no,
            brand_name=brand_name,
            brand_context=brand_row
        )
        print(f"[{_now()}] [B{idx}:{brand_no}] metric done | elapsed={_elapsed_ms(t_m)}ms")

        # 4-2) 요약 보고서
        t_s = time.time()
        summary_out = run_summary_report(
            sido=sido,
            sigungu=sigungu,
            industry=req.get("category") or "",
            budget_excl_rent=req.get("budget_bucket") or "",
            needs=req.get("needs_query") or "",
            brand_no=brand_no,
            brand_name=brand_name,
            metric_analysis=metric_out.get("지표분석_JSON", {}) if isinstance(metric_out, dict) else {},
            brand_row=brand_row,
            brand_context=brand_row,
        )
        summary_out = to_json_safe(summary_out)
        print(f"[{_now()}] [B{idx}:{brand_no}] summary done | elapsed={_elapsed_ms(t_s)}ms")
        # run_report.py, summary 직후
        print(f"[DBG] metric_out keys: {list(metric_out.keys()) if isinstance(metric_out, dict) else type(metric_out)}")
        print(f"[DBG] summary_out preview: {json.dumps(summary_out, ensure_ascii=False) if isinstance(summary_out, dict) else type(summary_out)}")


        # 4-3) 뉴스 요약
        t_n = time.time()
        news_out = run_news_summary(brand_no=brand_no, brand_name=brand_name, brand_context=brand_row)
        news_out = to_json_safe(news_out)
        print(f"[{_now()}] [B{idx}:{brand_no}] news done | elapsed={_elapsed_ms(t_n)}ms")

        results.append({
            "brand_no": brand_no,
            "brand_name": brand_name,
            "report_summary": summary_out,
            "news_summary": news_out,
            "rerank_score": rerank_score,
        })

        print(f"[{_now()}] [B{idx}:{brand_no}] all done | subtotal_elapsed={_elapsed_ms(t_m)}ms(metric)+{_elapsed_ms(t_s)}ms(summary)+{_elapsed_ms(t_n)}ms(news)")

    print(f"[{_now()}] [PIPE] end | total_elapsed={_elapsed_ms(t0)}ms | brands={len(results)}")
    return results


# =========================================
# CLI 엔트리
# =========================================
def main():
    ap = argparse.ArgumentParser(description="Brand recommendation → metric → summary → news (Serverless)")
    ap.add_argument("--input", type=str, help="요청 JSON 파일 경로", required=False)
    ap.add_argument("--output", type=str, help="결과 JSON 저장 경로", required=False)
    args = ap.parse_args()

    if args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            req = json.load(f)
    else:
        req = DEFAULT_REQUEST

    out = run_pipeline(req)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"[OK] saved → {args.output}")
    else:
        print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
