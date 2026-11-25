"""RAG over the data catalog, glossary and runbook with FAISS.

Embedders
- HashEmbedder (default, offline): hashed word 1-2-gram vectors. Lexical, not semantic; used so the
  project runs and tests without network or API keys.
- VertexEmbedder (EMBEDDER=vertex): Vertex AI text embeddings for semantic retrieval. Not run in CI.
"""
import os
import re
import numpy as np
import faiss
from sklearn.feature_extraction.text import HashingVectorizer
from .. import config as C


class HashEmbedder:
    dim = 1024

    def __init__(self):
        self.v = HashingVectorizer(n_features=self.dim, ngram_range=(1, 2), alternate_sign=False,
                                   norm="l2", stop_words="english", token_pattern=r"(?u)\b[\w\.]{2,}\b")

    def embed(self, texts):
        return self.v.transform(texts).toarray().astype("float32")


class VertexEmbedder:  # pragma: no cover - needs GCP credentials
    dim = 768

    def __init__(self, model="text-embedding-005"):
        from vertexai.language_models import TextEmbeddingModel
        self.m = TextEmbeddingModel.from_pretrained(model)

    def embed(self, texts):
        vecs = np.array([e.values for e in self.m.get_embeddings(list(texts))], dtype="float32")
        faiss.normalize_L2(vecs)
        return vecs


def make_embedder():
    return VertexEmbedder() if os.environ.get("EMBEDDER") == "vertex" else HashEmbedder()


def chunk_markdown(text: str) -> list[str]:
    parts = re.split(r"\n(?=#{2,3} )", text)
    return [p.strip() for p in parts if len(p.strip()) > 20]


class DocIndex:
    def __init__(self, chunks: list[str], embedder=None):
        self.chunks = chunks
        self.emb = embedder or make_embedder()
        mat = self.emb.embed(chunks)
        self.index = faiss.IndexFlatIP(mat.shape[1])
        self.index.add(mat)

    @classmethod
    def from_reports(cls, hide_tables=()):
        chunks = []
        for f in (C.REPORTS / "catalog.md", C.ROOT / "docs" / "runbook.md"):
            if f.exists():
                chunks += chunk_markdown(f.read_text())
        chunks = [c for c in chunks if not any(f"Table {t}" in c.splitlines()[0] or c.startswith(f"### Quarantine") and t == "quarantine_orders"
                                               for t in hide_tables)]
        return cls(chunks)

    def search(self, query: str, k: int = 3) -> list[dict]:
        q = self.emb.embed([query])
        scores, ids = self.index.search(q, min(k, len(self.chunks)))
        return [{"score": round(float(s), 3), "text": self.chunks[i][:600]} for s, i in zip(scores[0], ids[0]) if i >= 0 and s > 0]
