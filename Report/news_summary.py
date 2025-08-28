# Databricks notebook source
# Report/news_summary.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import re, json, time, urllib.parse, requests

# ── HTML/본문 추출 보조(옵션 의존성은 try-import)
try:    import trafilatura
except: trafilatura = None
try:    from readability import Document
except: Document = None

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
NAVER_NEWS_ENDPOINT = "https://openapi.naver.com/v1/search/news.json"

def _clean_html(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s or "")).strip()

def _parse_pubdate(s: Optional[str]):
    try: return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %z")
    except: return None

def _extract_domain(u: str):
    try: return urllib.parse.urlparse(u).netloc
    except: return None

def _normalize_url(u: str) -> str:
    try:
        p = urllib.parse.urlparse(u)
        qs = {k:v for k,v in urllib.parse.parse_qs(p.query).items() if not k.lower().startswith("utm_")}
        return urllib.parse.urlunparse((p.scheme,p.netloc,p.path,p.params,urllib.parse.urlencode(qs,doseq=True),""))
    except: return u

def _fetch_naver_news_recent(brand: str, client_id: str, client_secret: str,
                             days: int, per_page: int, pages: int,
                             include_corp: bool, corp_name: Optional[str]) -> List[Dict[str, Any]]:
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "User-Agent": USER_AGENT,
    }
    queries = [f"\"{brand}\""]
    no_space = brand.replace(" ", "")
    if no_space != brand: queries.append(no_space)
    if include_corp and corp_name: queries.append(f"\"{corp_name}\"")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    all_items: List[Dict[str, Any]] = []

    for q in queries:
        for p in range(pages):
            start = 1 + p * per_page
            r = requests.get(
                NAVER_NEWS_ENDPOINT, headers=headers,
                params={"query": q, "sort": "date", "display": min(per_page, 100), "start": start},
                timeout=20
            )
            if r.status_code != 200:
                continue
            items = r.json().get("items", [])
            for it in items:
                title = _clean_html(it.get("title", ""))
                desc  = _clean_html(it.get("description", ""))
                link_raw = it.get("originallink") or it.get("link") or ""
                link = _normalize_url(link_raw)
                pubd = _parse_pubdate(it.get("pubDate"))
                if pubd and pubd < cutoff:
                    continue
                all_items.append({
                    "title": title, "description": desc, "url": link,
                    "pub_date": pubd.isoformat() if pubd else None,
                    "publisher": _extract_domain(link), "query": q,
                })
            time.sleep(0.12)

    # URL 중복 제거
    seen, uniq = set(), []
    for x in all_items:
        if x["url"] in seen: continue
        seen.add(x["url"]); uniq.append(x)

    def _key(x):
        try: return datetime.fromisoformat(x["pub_date"]) if x["pub_date"] else datetime.min.replace(tzinfo=timezone.utc)
        except Exception: return datetime.min.replace(tzinfo=timezone.utc)
    uniq.sort(key=_key, reverse=True)
    return uniq

def _fetch_article_text(url: str, timeout: int = 20) -> Tuple[str, Optional[str]]:
    try:
        html = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout).text
    except Exception:
        return "", None
    if trafilatura:
        try:
            txt = trafilatura.extract(html)
            if txt and len(txt.split()) > 50: return txt.strip(), None
        except Exception: pass
    if Document:
        try:
            doc = Document(html)
            text = re.sub(r"<[^>]+>", " ", doc.summary())
            if len(text.split()) > 50: return text.strip(), doc.short_title()
        except Exception: pass
    return "", None

def _batch_fetch_fulltexts(items: List[Dict[str, Any]], sleep_sec: float = 0.4) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        txt, ht = _fetch_article_text(it["url"])
        ni = dict(it); ni["fulltext"] = txt; ni["extracted_title_fallback"] = ht
        out.append(ni); time.sleep(sleep_sec)
    return out

def run_news_summary(
     brand_no: str,
     spark,
     openai_client,
     mgenai_module,
     *,
     naver_client_id: str,
     naver_client_secret: str,
     per_brand_max_items: int = 20,
     days: int = 14,
     per_page: int = 20,
     pages: int = 4,
     prompt_uri: str = "prompts:/gold.default.franchise_news_summarizing_prompt/4",
     openai_model: str = "gpt-5-mini",
 ) -> Dict[str, Any]:
     import mlflow
     from pyspark.sql import functions as F


    brand_df = (spark.table("gold.master.franchise_mois")
                     .filter(F.col("공정위_등록번호") == brand_no)
                     .limit(1))
    rows = brand_df.collect()
    if not rows:
        raise ValueError(f"브랜드 행 없음: 공정위_등록번호={brand_no}")
    row = rows[0].asDict()
    brand_name = row.get("공정위_영업표지_브랜드명") or ""
    corp_name  = (row.get("본사_상호명") or "").strip()

    brand_ctx_cols = [
        "공정위_등록번호","공정위_영업표지_브랜드명","공정위_업종분류",
        "배민_업종분류","본사_상호명","브랜드_소개내용_1","브랜드_장점및노하우",
    ]
    brand_ctx = {k: row.get(k) for k in brand_ctx_cols if k in row}
    brand_ctx_json = json.dumps(brand_ctx, ensure_ascii=False)

    items = _fetch_naver_news_recent(
        brand=brand_name, client_id=naver_client_id, client_secret=naver_client_secret,
        days=days, per_page=per_page, pages=pages,
        include_corp=bool(corp_name), corp_name=corp_name if corp_name else None
    )
    if len(items) > per_brand_max_items:
        items = items[:per_brand_max_items]
    items_full = _batch_fetch_fulltexts(items)

    # LLM 프롬프트 로드
    p = mgenai_module.load_prompt(prompt_uri)
    tmpl = getattr(p, "template", p)
    if isinstance(tmpl, dict):
        system_src = tmpl.get("system", "")
        user_src   = tmpl.get("user", "") or tmpl.get("template", "")
    elif isinstance(tmpl, str):
        try:
            j = json.loads(tmpl)
            system_src = j.get("system", "") if isinstance(j, dict) else ""
            user_src   = (j.get("user", "") or j.get("template", "")) if isinstance(j, dict) else str(j)
        except Exception:
            system_src = ""; user_src = tmpl
    else:
        system_src = ""; user_src = str(tmpl)
    if not user_src:
        raise ValueError("user 프롬프트 템플릿이 비어 있습니다.")

    # 입력 구성
    arts = []
    for i, it in enumerate(items_full, 1):
        a = dict(it); a["id"] = i
        if a.get("fulltext"):
            a["fulltext"] = a["fulltext"][:1500]
        arts.append(a)

    # OpenAI 호출
    def _call(model, instructions, user_input, max_tokens=4000):
        try:
            return openai_client.responses.create(
                model=model, instructions=instructions, input=user_input,
                reasoning={"effort": "minimal"}, text={"verbosity": "low"},
                response_format={"type": "json_object"}, max_output_tokens=max_tokens,
            )
        except TypeError:
            return openai_client.responses.create(
                model=model, instructions=instructions, input=user_input,
                reasoning={"effort": "minimal"}, text={"verbosity": "low"},
                max_output_tokens=max_tokens,
            )

    user_prompt = user_src.format(
        brand=brand_name,
        articles_json=json.dumps(arts, ensure_ascii=False),
        brand_context_json=brand_ctx_json,
    )
    resp = _call(openai_model, system_src, user_prompt, 4000)
    raw = (getattr(resp, "output_text", "") or "").strip()
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    candidate = m.group(0) if m else raw
    if not candidate.endswith("}"): candidate += "}"
    try:
        result = json.loads(candidate)
    except Exception:
        result = {"raw_text": raw, "_parse_error": True}

    # MLflow 저장
    with mlflow.start_run(nested=True):
        mlflow.log_param("brand_no", brand_no)
        mlflow.log_param("brand_name", brand_name)
        mlflow.log_param("prompt_uri", prompt_uri)
        mlflow.log_param("openai_model", openai_model)
        mlflow.log_metric("queried_count", len(items))
        mlflow.log_metric("selected_count", len(result.get("selected") or []))
        mlflow.log_dict(
            {"brand_no": brand_no, "brand_name": brand_name, "brand_row": row,
             "items": items_full, "result": result},
            f"franchise_news_{brand_no}_{brand_name}.json"
        )
    return {"brand_no": brand_no, "brand_name": brand_name, "news_result": result}

