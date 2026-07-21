"""
Narrative embeddings index over ref_safe__wave_memory.

ref_safe__wave_memory (written by llm.py::_write_wave_memory) already holds a
small, clean, grounding-validated 3-4 sentence narrative summary per wave —
a much better retrieval target for "what did we say about X last wave"
questions than raw report HTML (markup + base64-encoded chart PNGs). This
module embeds that corpus and answers similarity queries against it.

The corpus is small by construction (one row per wave, a few dozen total) —
brute-force cosine similarity in numpy is simpler and lower-risk here than
installing DuckDB's VSS extension for no real performance gain at this scale.

Usage:
  python reports/narrative_index.py    # (re)build the embeddings table

Required environment variables:
  MOTHERDUCK_TOKEN, MISTRAL_API_KEY
"""

from datetime import datetime as _dt

import numpy as np

from cost import _Usage, _mistral_client, _track_cost
from db import PROD_SCHEMA, _get_connection

EMBED_MODEL = "mistral-embed"


def build_index(con=None, cost_tracker: dict | None = None, schema: str = PROD_SCHEMA) -> int:
    """(Re)build {schema}.ref_safe__narrative_embeddings from ref_safe__wave_memory.

    Re-embeds every row each call rather than diffing incrementally — the
    corpus is small enough (a few dozen rows max) that this is simpler and
    the embedding cost is trivial. Returns the number of rows embedded.
    """
    owns_con = con is None
    if owns_con:
        con = _get_connection()
    if cost_tracker is None:
        cost_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
    try:
        rows = con.execute(
            f"SELECT wave_number, notable_summary FROM {schema}.ref_safe__wave_memory "
            "ORDER BY wave_number"
        ).fetchall()
        if not rows:
            print("No rows in ref_safe__wave_memory — nothing to embed.")
            return 0

        client = _mistral_client()
        texts = [r[1] for r in rows]
        resp = client.embeddings.create(model=EMBED_MODEL, inputs=texts)
        _track_cost(cost_tracker, EMBED_MODEL, _Usage(resp.usage.prompt_tokens or 0, 0))

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.ref_safe__narrative_embeddings (
                wave_number INTEGER PRIMARY KEY,
                notable_summary TEXT,
                embedding FLOAT[],
                model_id TEXT,
                indexed_at TIMESTAMP
            )
        """)
        now = _dt.utcnow()
        for (wave_number, summary), item in zip(rows, resp.data):
            con.execute(
                f"INSERT OR REPLACE INTO {schema}.ref_safe__narrative_embeddings VALUES (?,?,?,?,?)",
                [wave_number, summary, item.embedding, EMBED_MODEL, now],
            )
        print(f"Embedded {len(rows)} wave(s) into {schema}.ref_safe__narrative_embeddings")
        return len(rows)
    finally:
        if owns_con:
            con.close()


def retrieve_narrative(query: str, con, k: int = 5, schema: str = PROD_SCHEMA) -> list[dict]:
    """Return the k most similar wave_memory rows to `query` by cosine similarity.

    Each result: {wave_number, notable_summary, similarity}, sorted by
    similarity descending. Empty list if the index hasn't been built yet.
    """
    client = _mistral_client()
    resp = client.embeddings.create(model=EMBED_MODEL, inputs=[query])
    query_vec = np.array(resp.data[0].embedding, dtype=np.float64)
    query_norm = np.linalg.norm(query_vec)

    rows = con.execute(
        f"SELECT wave_number, notable_summary, embedding "
        f"FROM {schema}.ref_safe__narrative_embeddings"
    ).fetchall()
    if not rows:
        return []

    scored = []
    for wave_number, summary, embedding in rows:
        vec = np.array(embedding, dtype=np.float64)
        denom = query_norm * np.linalg.norm(vec)
        similarity = float(np.dot(query_vec, vec) / denom) if denom else 0.0
        scored.append({
            "wave_number": wave_number,
            "notable_summary": summary,
            "similarity": similarity,
        })

    scored.sort(key=lambda r: r["similarity"], reverse=True)
    return scored[:k]


def main() -> None:
    build_index()


if __name__ == "__main__":
    main()
