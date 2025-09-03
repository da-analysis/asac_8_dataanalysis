# recommender.py
import os, json
from typing import List, Dict, Optional, Tuple

# (선택) .env 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ==== 비밀키/접속정보 ====
QDRANT_URL     = os.getenv("QDRANT_URL", "")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ==== 콜렉션/모델 설정 ====
COLL         = "franchise_docs_uc"
VECTOR_NAME  = "text"
E5_MODEL_ID  = "intfloat/multilingual-e5-base"
BGE_MODEL_ID = "BAAI/bge-reranker-v2-m3"

# ==== 라이브러리 ====
import torch
from transformers import AutoTokenizer, AutoModel
from qdrant_client import QdrantClient
from qdrant_client.http import models

# ---------------------------------------
# 유틸
# ---------------------------------------
def parse_budget_bucket(bucket: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """UI의 비용구간 문자열 -> (min, max) 만원단위로 변환"""
    if not bucket:
        return (None, None)
    m = {
        "3천만원 이하": (None, 3000.0),
        "3천만원~6천만원": (3000.0, 6000.0),
        "6천만원~8천만원": (6000.0, 8000.0),
        "8천만원~1억": (8000.0, 10000.0),
        "1억~1억5천만원": (10000.0, 15000.0),
        "1억5천만원 이상": (15000.0, None),
    }
    return m.get(bucket, (None, None))

# ---------------------------------------
# 모델 래퍼
# ---------------------------------------
class E5Embedder:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained(E5_MODEL_ID)
        self.model = AutoModel.from_pretrained(E5_MODEL_ID)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(self.device).eval()

    @torch.inference_mode()
    def encode_query(self, query: str) -> List[float]:
        enc = self.tokenizer([f"query: {query}"], padding=True, truncation=True, max_length=256, return_tensors="pt").to(self.device)
        out = self.model(**enc)
        H = out.last_hidden_state
        mask = enc["attention_mask"].unsqueeze(-1).expand(H.size()).float()
        emb = (H * mask).sum(1) / mask.sum(1).clamp(min=1e-9)
        emb = torch.nn.functional.normalize(emb, p=2, dim=1)
        return emb.cpu().numpy()[0].tolist()

class MultiQueryGenerator:
    """OpenAI로 의미 보존 다변화 쿼리 생성. 키 없으면 단일 쿼리로 동작."""
    def __init__(self, model: str, api_key: Optional[str]):
        self.model = model
        self.client = None
        if api_key:
            import openai as openai_module
            self.client = openai_module.OpenAI(api_key=api_key)

    def generate(self, seed: str, n: int) -> List[str]:
        if not self.client:
            return [seed]
        sys = "프랜차이즈 검색용. 의미 보존, 표현만 다양화. JSON 배열로만 응답."
        usr = f'원문: "{seed}"\n{n}개의 변형. 형식: ["a","b","c"]'
        try:
            r = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role":"system","content":sys},{"role":"user","content":usr}],
                max_tokens=200, temperature=0.0
            )
            arr = json.loads(r.choices[0].message.content.strip())
            outs = [s for s in arr if isinstance(s, str) and s.strip()]
            if seed not in outs:
                outs = [seed] + outs
            return outs[:max(1, n)]
        except Exception:
            return [seed]

class BGECrossEncoderReranker:
    """BGE 리랭커 우선 사용, 실패 시 가벼운 MiniLM CrossEncoder 사용."""
    def __init__(self, device: Optional[str] = None, batch_size: int = 32, pair_trunc: int = 512):
        self.pair_trunc = pair_trunc
        self.batch_size = batch_size
        self.is_bge = False
        try:
            from FlagEmbedding import FlagReranker
            if device is None:
                device = "cuda" if torch.cuda.is_available() else "cpu"
            self.model = FlagReranker(BGE_MODEL_ID, use_fp16=True, device=device)
            self.is_bge = True
        except Exception:
            from sentence_transformers import CrossEncoder
            self.model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

    def rerank(self, query: str, docs: List[Dict], top_k: int):
        if not docs:
            return []
        pairs = []
        for d in docs:
            txt = f"{d.get('brand_name','')} {d.get('category','')} {d.get('chunk_text','')}"
            pairs.append([query, txt[:self.pair_trunc]])  # 입력 길이 트렁케이션
        if self.is_bge:
            scores = self.model.compute_score(pairs, batch_size=self.batch_size)
            if isinstance(scores, (int, float)):
                scores = [scores]
        else:
            scores = self.model.predict(pairs)
        scored = list(zip(docs, [float(s) for s in scores]))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

# ---------------------------------------
# 검색 시스템
# ---------------------------------------
class SearchSystem:
    def __init__(self):
        if not QDRANT_URL or not QDRANT_API_KEY:
            raise RuntimeError("QDRANT_URL / QDRANT_API_KEY 환경변수 필요")
        self.qc = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, prefer_grpc=True, timeout=10.0)
        self.embedder = E5Embedder()
        self.mq = MultiQueryGenerator(OPENAI_MODEL, OPENAI_API_KEY)

    def _build_filter(self,
                      category: Optional[str],
                      regions: Optional[List[str]],
                      min_m: Optional[float],
                      max_m: Optional[float]) -> Optional[models.Filter]:
        must = []
        if category:
            must.append(models.FieldCondition(key="category", match=models.MatchValue(value=category)))
        if (min_m is not None) or (max_m is not None):
            rng = {}
            if min_m is not None: rng["gte"] = float(min_m) * 10000  # 만원→원
            if max_m is not None: rng["lte"] = float(max_m) * 10000
            must.append(models.FieldCondition(key="startup_cost", range=models.Range(**rng)))
        if regions:
            should = [models.FieldCondition(key="regions", match=models.MatchValue(value=r)) for r in regions]
            must.append(models.Filter(should=should))  # OR 조건을 must로 강제
        return models.Filter(must=must) if must else None

    def _vector_search(self, qv: List[float], qfilter: Optional[models.Filter], limit: int) -> List[Dict]:
        # 당신 환경에서 잘 되던 방식 유지 (Deprecation 경고 가능)
        hits = self.qc.search(
            collection_name=COLL,
            query_vector=(VECTOR_NAME, qv),
            query_filter=qfilter,
            limit=limit,
            with_payload=True
        )
        out: List[Dict] = []
        for h in hits:
            pl = h.payload or {}
            d = {
                "int_id": int(pl.get("int_id") or 0),
                "franchise_id": str(pl.get("franchise_id") or ""),
                "brand_name": str(pl.get("brand_name") or ""),
                "category": str(pl.get("category") or ""),
                "startup_cost": float(pl["startup_cost"]) if pl.get("startup_cost") is not None else None,
                "regions": list(pl.get("regions") or []),
                "chunk_text": str(pl.get("chunk_text") or ""),
                "vector_score": float(getattr(h, "score", 0.0)),
                "matched_query": "",
            }
            out.append(d)
        return out

    def recommend_brand_ids(self,
                            regions: Optional[List[str]],
                            category: Optional[str],
                            budget_bucket: Optional[str],
                            needs_query: str,
                            top_k: int = 2,
                            # ----- 성능 노브들 -----
                            multi_query_count: int = 3,
                            initial_search_limit: int = 50,
                            rerank_topk: int = 32,
                            pair_trunc: int = 512,
                            ce_batch: int = 32,
                            ce_device: Optional[str] = None) -> List[str]:

        # 1) 필터 구성
        min_m, max_m = parse_budget_bucket(budget_bucket)
        qfilter = self._build_filter(category, regions, min_m, max_m)

        # 2) 멀티쿼리 (키 없으면 단일)
        queries = self.mq.generate(needs_query, multi_query_count)

        # 3) 후보 수집 (쿼리별 벡터 검색)
        all_hits: List[Dict] = []
        for q in queries:
            qv = self.embedder.encode_query(q)
            raw_hits = self._vector_search(qv, qfilter, limit=initial_search_limit)
            for d in raw_hits:
                d = dict(d)
                d["matched_query"] = q
                all_hits.append(d)

        # 4) 문서(int_id) 단위 dedup (최고 벡터점수 유지)
        by_doc: Dict[int, Dict] = {}
        for d in all_hits:
            did = d.get("int_id", 0)
            if did == 0: 
                continue
            if (did not in by_doc) or (d["vector_score"] > by_doc[did]["vector_score"]):
                by_doc[did] = d
        unique_docs = list(by_doc.values())
        if not unique_docs:
            return []

        # 5) 리랭크 후보 제한 (벡터점수 내림차순 → 상위 rerank_topk)
        unique_docs.sort(key=lambda x: x["vector_score"], reverse=True)
        seed = unique_docs[:min(rerank_topk, len(unique_docs))]

        # 6) 리랭크 실행
        reranker = BGECrossEncoderReranker(device=ce_device, batch_size=ce_batch, pair_trunc=pair_trunc)
        reranked = reranker.rerank(needs_query, seed, top_k=len(seed))

        # 7) 같은 브랜드 통합(최고 리랭크 점수 유지)
        by_brand: Dict[str, Dict] = {}
        for doc, score in reranked:
            brand = doc.get("brand_name", "")
            if not brand:
                continue
            prev = by_brand.get(brand)
            if (prev is None) or (score > prev["score"]):
                by_brand[brand] = {"doc": doc, "score": float(score)}

        ranked = sorted(by_brand.values(), key=lambda x: x["score"], reverse=True)
        results = []
        for x in ranked[:top_k]:
            doc = x["doc"]
            results.append({
                "brand_no": str(doc.get("franchise_id", "")),
                "brand_name": doc.get("brand_name", ""),
                "rerank_score": float(x["score"]),
                # 선택: 디버깅용 원점수들 유지 가능
                "vector_score": float(doc.get("vector_score", 0.0)),
            })
        print(f"[RECOMMENDER] rerank done | k={len(results)} | brands={[r['brand_no'] for r in results]}")
        return results

# ---------------------------------------
# 싱글턴 + 외부용 엔트리 포인트
# ---------------------------------------
_system: Optional[SearchSystem] = None
def _get_system() -> SearchSystem:
    global _system
    if _system is None:
        _system = SearchSystem()
    return _system

def recommend_brand_ids(
    regions: Optional[List[str]],
    category: Optional[str],
    budget_bucket: Optional[str],
    needs_query: str,
    top_k: int = 2,
    # 성능/튜닝 노브
    multi_query_count: int = 3,
    initial_search_limit: int = 50,
    rerank_topk: int = 32,
    pair_trunc: int = 512,
    ce_batch: int = 32,
    ce_device: str | None = None,
) -> List[str]:
    """FastAPI에서 직접 호출하는 외부 API. 실제 엔진 호출."""
    sys = _get_system()
    return sys.recommend_brand_ids(
        regions=regions,
        category=category,
        budget_bucket=budget_bucket,
        needs_query=needs_query,
        top_k=top_k,
        multi_query_count=multi_query_count,
        initial_search_limit=initial_search_limit,
        rerank_topk=rerank_topk,
        pair_trunc=pair_trunc,
        ce_batch=ce_batch,
        ce_device=ce_device,
    )
