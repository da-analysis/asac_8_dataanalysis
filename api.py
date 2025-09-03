# api.py
import os
from typing import Optional, List, Dict, Any
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from datetime import datetime, timedelta
from run_report import run_pipeline as run_main_pipeline # <-- 핵심

# api.py 상단
from fastapi.encoders import jsonable_encoder
import numpy as np
from datetime import datetime, date
from decimal import Decimal
# 기존의 recommend_brand_ids 임포트는 남겨두어도 되지만, 이제는 run_pipeline이 주 역할


# === 너의 추천 로직 (엔진) 불러오기 ===
# recommender.py에 recommend_brand_ids(regions, category, budget_bucket, needs_query, top_k) 가 있어야 함
from Recommendation.recommender import recommend_brand_ids

# -----------------------------------------------------
# FastAPI App
# -----------------------------------------------------
app = FastAPI(title="FRAI Backend")

# 정적 프론트 서빙 (원본 디자인 파일 그대로 사용)
# 폴더 구조:
#   public/index.html
#   public/static/app.js
#   public/static/styles.css
app.mount("/static", StaticFiles(directory="public/static"), name="static")

@app.get("/")
def root():
    return FileResponse("public/index.html")

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now().isoformat()}

# -----------------------------------------------------
# 요청/응답 스키마 (프론트 기대 구조에 맞춤)
# -----------------------------------------------------
class RecommendReq(BaseModel):
    sido: str
    sigungu: str
    industry: str
    budget_excl_rent: str
    needs: str
    top_k: Optional[int] = 2

# 프론트의 displayResults에서 기대하는 구조:
# data = {
#   "user_input": {...},
#   "recommended_brands": [ { brand_no, brand_name, recommendation_level, summary_report, news_analysis }, ... ],
#   "analysis_summary": "..."
# }
# (app.js 내부 렌더 로직 참조) :contentReference[oaicite:4]{index=4}

# -----------------------------------------------------
# 유틸: 요약/뉴스 섹션을 당장은 "형태 보장" 위주로 제공
# 나중에 Report/metric_analysis.py, report_summary.py, news_summary.py 붙이면 대체 가능
# -----------------------------------------------------
def to_json_safe(o):
    import numpy as np
    from datetime import datetime, date
    from decimal import Decimal

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

def _fake_summary_report(brand_no: str, brand_name: str) -> Dict[str, Any]:
    # 프론트의 createSummaryReportSection에서 요구하는 key들 반환 :contentReference[oaicite:5]{index=5}
    return {
        "brand_summary": f"{brand_name} (브랜드번호 {brand_no})의 핵심 요약입니다.",
        "investment_analysis": "초기 투자 비용과 지출 구조에 대한 간단한 요약.",
        "profitability_analysis": "매출, 마진, 회수기간 등에 대한 가벼운 관찰.",
        "market_analysis": "입지 특성, 주변 경쟁도 등 시장 요인 개관.",
        "risk_analysis": "원가/공급, 상권 리스크, 본사 정책 변화 리스크.",
        "recommendation": "창업자 니즈와 맞는지 1-2줄 코멘트."
    }

def _fake_news_block(brand_name: str) -> Dict[str, Any]:
    # 프론트의 createNewsAnalysisSection에서 요구하는 구조 맞춤(없으면 빈 값) :contentReference[oaicite:6]{index=6}
    today = datetime.now()
    items = []
    for i in range(3):  # 예시 3개
        items.append({
            "title": f"{brand_name} 관련 업계 동향 {i+1}",
            "link": "https://news.example.com",
            "pubDate": (today - timedelta(days=i+1)).strftime("%Y-%m-%d"),
            "description": f"{brand_name}에 대한 간단 기사 요약 {i+1}"
        })
    return {
        "overall_summary": f"{brand_name} 뉴스 전반 요약 (데모).",
        "news_items": items,
        "individual_summaries": [f"{brand_name} 기사 {i+1} 요약" for i in range(len(items))]
    }

def _recommendation_level(rank_idx: int) -> str:
    return "high" if rank_idx < 2 else ("medium" if rank_idx < 4 else "low")

def adapt_news_for_front(ns: dict) -> dict:
    if not isinstance(ns, dict):
        return {"overall_summary": "", "news_items": [], "individual_summaries": []}

    items_src = ns.get("selected") or ns.get("news_items") or []
    news_items, indiv = [], []

    for it in items_src if isinstance(items_src, list) else []:
        title = it.get("title") or ""
        link  = it.get("url") or it.get("link") or ""
        pubd  = it.get("pub_date") or it.get("pubDate") or ""
        desc  = it.get("summary") or it.get("description") or ""
        news_items.append({
            "title": title,
            "link": link,
            "pubDate": pubd,
            "description": desc,
        })
        indiv.append(desc)

    overall = ns.get("overall_summary") or ns.get("summary") or ""
    return {
        "overall_summary": overall,
        "news_items": news_items,
        "individual_summaries": indiv,
    }

def adapt_summary_for_front_kor(sr: Any, *, brand_name: str = "") -> Dict[str, Any]:
    """
    Report/summary의 한글 키들을 프론트 6섹션으로 매핑.
    표시 순서 요구사항을 반영해 문자열을 조립한다.
    """
    out = {
        "brand_summary": "",
        "investment_analysis": "",
        "profitability_analysis": "",
        "market_analysis": "",
        "risk_analysis": "",
        "recommendation": "",
    }
    if not isinstance(sr, dict):
        out["brand_summary"] = str(sr) if sr else ""
        return out

    # ---------- 1) 창업비용 (요약 + 세부) ----------
    lines: List[str] = []

    # (1) 요약
    cost_sum = sr.get("창업비용_요약")
    if cost_sum:
        # 굵은 제목 느낌만 주고 특수괄호는 제거
        lines.append("## 창업비용 요약")
        lines.append(cost_sum)
        lines.append("")  # 빈 줄

    # (2) 세부
    cost_det = sr.get("창업비용_세부") or {}
    if isinstance(cost_det, dict):
        # 체크 아이콘 라인 빌더
        def add_check(label: str, value: Any):
            if value:
                lines.append(f"✅ {label}: {value}")

        lines.append("## 창업비용 세부")
        add_check("최초가맹비용", cost_det.get("최초가맹비용"))
        add_check("기타비용", cost_det.get("기타비용"))
        add_check("로열티", cost_det.get("로얄티"))
        add_check("광고/판촉비", cost_det.get("광고_판촉비"))
        lines.append("")  # 빈 줄
        # 추천 임대료 묶음
        recs = cost_det.get("추천임대료_및_이유") or []
        if isinstance(recs, list) and recs:
            lines.append("## 🏠 추천 임대료 (면적)")
            for r in recs:
                if not isinstance(r, dict):
                    continue
                점포규모 = r.get("점포규모") or ""
                면적구간 = r.get("면적구간_중앙값_m2") or ""
                상권 = r.get("상권") or ""
                조사층 = r.get("조사층") or r.get("조사 층") or ""
                월임대료 = r.get("월임대료") or "N/A"
                임대보증금 = r.get("임대보증금") or "N/A"
                이유 = r.get("이유") or ""
                # lines.append(
                #     f"• ({점포규모}/{면적구간}) {상권} {조사층} | 월임대료 {월임대료}, 보증금 {임대보증금} | 사유: {이유}"
                # )
                lines.append(f"[{r.get('점포규모')}] {r.get('상권')} ({r.get('면적구간_중앙값_m2')}" + "m²)")
                lines.append(f"  - 월임대료: {r.get('월임대료')}")
                lines.append(f"  - 보증금: {r.get('임대보증금')}")
                lines.append(f"  - 사유: {r.get('이유')}")
                lines.append("")  # 빈 줄

    # 최종 합치기 (None 제거)
    out["investment_analysis"] = "\n".join([ln for ln in lines if ln is not None])

    # ---------- 2) 매출 요약 ----------
    out["profitability_analysis"] = sr.get("매출_요약") or sr.get("수익성_요약") or ""

    # ---------- 3) 브랜드 강점 ----------
    strengths = sr.get("브랜드_강점")
    if isinstance(strengths, list) and strengths:
        out["market_analysis"] = "\n".join(f"{s}" for s in strengths) #"브랜드 강점\n" + 
    elif isinstance(strengths, str):
        out["market_analysis"] = strengths # "브랜드 강점\n" + 

    # ---------- 4) 브랜드 리스크 ----------
    risks = sr.get("브랜드_리스크")
    if isinstance(risks, list) and risks:
        out["risk_analysis"] = "\n".join(f"{r}" for r in risks) # "브랜드 리스크\n" + 
    elif isinstance(risks, str):
        out["risk_analysis"] = risks # "브랜드 리스크\n" + 

    # ---------- 5) 권고사항 ----------
    reco = sr.get("권고사항")
    if isinstance(reco, list) and reco:
        out["recommendation"] = "\n".join(f"{i+1}. {x}" for i, x in enumerate(reco)) # "권고사항\n" + 
    elif isinstance(reco, str):
        out["recommendation"] = reco # "권고사항\n" + 

    # ---------- 6) 키워드 패스스루 ----------
    if sr.get("브랜드_소개_및_강점_키워드"):
        out["브랜드_소개_및_강점_키워드"] = sr["브랜드_소개_및_강점_키워드"]
    if sr.get("투자분석_키워드"):
        out["투자분석_키워드"] = sr["투자분석_키워드"]
    if sr.get("실행전략_키워드"):
        out["실행전략_키워드"] = sr["실행전략_키워드"]

    # 보조 안내
    if not any(bool(v) for v in out.values()):
        out["brand_summary"] = f"{brand_name} 요약 데이터를 생성하지 못했습니다."

    return out

def _score_to_100_linear(s, minv=-3.0, maxv=0.0):
    try:
        s = float(s)
    except Exception:
        return None
    if s < minv: s = minv
    if s > maxv: s = maxv
    return round(((s - minv) / (maxv - minv)) * 100)

# -----------------------------------------------------
# 메인 추천 엔드포인트 (프론트 원본이 호출하는 경로/스키마) :contentReference[oaicite:7]{index=7}
# -----------------------------------------------------
@app.post("/api/recommendations")
def api_recommendations(req: RecommendReq):
    """
    프론트가 보낸 입력을 run_report.py의 run_pipeline으로 넘기고,
    프론트가 기대하는 스키마로 변환해서 돌려줍니다.
    """
    try:
        # 1) run_report 파이프라인 요청 스키마로 매핑
        pipeline_req = {
            # run_report는 regions(List[str])를 씁니다.
            "regions": [req.sido],           # 프론트에서 이미 '서울특별시'→'서울'로 변환됨
            "sido": req.sido,                # 보고서에 그대로 사용
            "sigungu": req.sigungu,
            "category": req.industry,
            "budget_bucket": req.budget_excl_rent,
            "needs_query": req.needs,
            "top_k": req.top_k or 2,
            # 튜닝 파라미터(선택): 필요 시 기본값 사용 or 노출
            # "multi_query_count": 3, "initial_search_limit": 50, ...
        }

        # 2) 메인 파이프라인 실행 (추천 → 지표분석 → 요약 → 뉴스요약)
        results = run_main_pipeline(pipeline_req)

        # 3) 프론트 스키마로 어댑트
        recommended = []
        # api.py /api/recommendations 내부 (결과 루프에서)
        for i, r in enumerate(results):
            brand_no = r.get("brand_no")
            brand_name = r.get("brand_name") or f"브랜드({brand_no})"

            summary_report_raw = r.get("report_summary")  # ← run_report의 한글 키 결과
            summary_report = adapt_summary_for_front_kor(summary_report_raw, brand_name=brand_name)

            news_analysis = adapt_news_for_front(r.get("news_summary") or {})  # 기존 유지

            # 만약 6개 섹션이 전부 빈 문자열인 경우(내용 0), 최소 더미라도 채워서 화면 보장
            if not any(bool(v) for v in summary_report.values()):
                summary_report = _fake_summary_report(brand_no, brand_name)  # 이미 존재하는 더미. :contentReference[oaicite:2]{index=2}
            score_raw = r.get("rerank_score")
            score_100 = _score_to_100_linear(score_raw)
            recommended.append({
                "brand_no": brand_no,
                "brand_name": brand_name,
                "recommendation_level": _recommendation_level(i),
                "summary_report": summary_report,     # 프론트가 읽는 키
                "news_analysis": news_analysis,
                "rerank_score": score_raw,   # 원점수 (예: 5 ~ -10)
                "score_100": score_100      # 0 ~ 100 스케일
            })

        payload = {
            "user_input": req.model_dump(),   # 프론트 측 요약 카드에서 사용
            "recommended_brands": recommended,
            "analysis_summary": "실데이터 파이프라인(run_report) 실행 완료."  # 상단 요약 박스
        }
        
        custom_encoder = {
            # numpy 계열
            np.integer: int,
            np.floating: float,
            np.ndarray: lambda x: x.tolist(),
            # 기타 안전 변환
            Decimal: float,
            datetime: lambda x: x.isoformat(),
            date: lambda x: x.isoformat(),
            set: list,
        }

        # safe_payload = jsonable_encoder(payload, custom_encoder=custom_encoder)
        safe_payload = to_json_safe(payload)

        # 디버그용: 혹시 numpy 남았는지 탐색
        def find_first_numpy(obj, path="root"):
            import numpy as np
            if isinstance(obj, np.ndarray):
                print("[NDARRAY]", path, f"shape={obj.shape}, dtype={obj.dtype}")
                return True
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if find_first_numpy(v, f"{path}.{k}"):
                        return True
            if isinstance(obj, (list, tuple)):
                for i, v in enumerate(obj):
                    if find_first_numpy(v, f"{path}[{i}]"):
                        return True
            return False

        find_first_numpy(safe_payload)
        return JSONResponse(content={"success": True, "data": safe_payload})

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"서버 오류: {e}"}
        )

# -----------------------------------------------------
# (선택) CORS가 필요할 때만 활성화 (다른 도메인/포트에서 호출 시)
# -----------------------------------------------------
# from fastapi.middleware.cors import CORSMiddleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost:5173","http://127.0.0.1:5173","http://localhost:8000"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
