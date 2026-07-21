from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from narrative_index import build_index, retrieve_narrative


def _embedding_response(vectors: list[list[float]], prompt_tokens: int = 42):
    data = []
    for v in vectors:
        item = MagicMock()
        item.embedding = v
        data.append(item)
    resp = MagicMock()
    resp.data = data
    resp.usage.prompt_tokens = prompt_tokens
    return resp


def _mock_con(fetch_rows):
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = fetch_rows
    return con


@pytest.fixture
def cost_tracker():
    return {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}


def test_build_index_embeds_every_wave_memory_row(cost_tracker):
    con = _mock_con([(37, "Slovak firms saw profit pressure ease."),
                      (38, "Access to finance tightened for Slovak SMEs.")])
    client = MagicMock()
    client.embeddings.create.return_value = _embedding_response([[0.1, 0.2], [0.3, 0.4]])

    with patch("narrative_index._mistral_client", return_value=client):
        n = build_index(con=con, cost_tracker=cost_tracker)

    assert n == 2
    client.embeddings.create.assert_called_once()
    _, kwargs = client.embeddings.create.call_args
    assert kwargs["inputs"] == [
        "Slovak firms saw profit pressure ease.",
        "Access to finance tightened for Slovak SMEs.",
    ]
    # CREATE TABLE + 2 INSERT OR REPLACE calls (plus the initial SELECT)
    insert_calls = [c for c in con.execute.call_args_list if "INSERT OR REPLACE" in c.args[0]]
    assert len(insert_calls) == 2


def test_build_index_tracks_cost(cost_tracker):
    con = _mock_con([(37, "summary text")])
    client = MagicMock()
    client.embeddings.create.return_value = _embedding_response([[0.1, 0.2]], prompt_tokens=100)

    with patch("narrative_index._mistral_client", return_value=client):
        build_index(con=con, cost_tracker=cost_tracker)

    assert cost_tracker["calls"] == 1
    assert cost_tracker["input_tokens"] == 100
    assert "mistral-embed" in cost_tracker["by_model"]


def test_build_index_no_rows_returns_zero(cost_tracker):
    con = _mock_con([])
    client = MagicMock()
    with patch("narrative_index._mistral_client", return_value=client):
        n = build_index(con=con, cost_tracker=cost_tracker)
    assert n == 0
    client.embeddings.create.assert_not_called()


def test_build_index_closes_connection_it_opened(cost_tracker):
    con = _mock_con([])
    client = MagicMock()
    with patch("narrative_index._mistral_client", return_value=client), \
         patch("narrative_index._get_connection", return_value=con):
        build_index(cost_tracker=cost_tracker)  # no con passed -> owns it
    con.close.assert_called_once()


def test_build_index_does_not_close_connection_it_was_given(cost_tracker):
    con = _mock_con([])
    client = MagicMock()
    with patch("narrative_index._mistral_client", return_value=client):
        build_index(con=con, cost_tracker=cost_tracker)
    con.close.assert_not_called()


def test_retrieve_narrative_ranks_by_cosine_similarity():
    # query vector [1, 0] — wave 38's [1, 0] is identical (similarity 1.0),
    # wave 37's [0, 1] is orthogonal (similarity 0.0)
    con = _mock_con([
        (37, "Orthogonal summary", [0.0, 1.0]),
        (38, "Matching summary", [1.0, 0.0]),
    ])
    client = MagicMock()
    client.embeddings.create.return_value = _embedding_response([[1.0, 0.0]])

    with patch("narrative_index._mistral_client", return_value=client):
        results = retrieve_narrative("query", con, k=5)

    assert results[0]["wave_number"] == 38
    assert results[0]["similarity"] == pytest.approx(1.0)
    assert results[1]["wave_number"] == 37
    assert results[1]["similarity"] == pytest.approx(0.0, abs=1e-9)


def test_retrieve_narrative_respects_k():
    rows = [(w, f"summary {w}", [float(w), 1.0]) for w in range(30, 40)]
    con = _mock_con(rows)
    client = MagicMock()
    client.embeddings.create.return_value = _embedding_response([[35.0, 1.0]])

    with patch("narrative_index._mistral_client", return_value=client):
        results = retrieve_narrative("query", con, k=3)

    assert len(results) == 3


def test_retrieve_narrative_empty_index_returns_empty_list():
    con = _mock_con([])
    client = MagicMock()
    client.embeddings.create.return_value = _embedding_response([[1.0, 0.0]])

    with patch("narrative_index._mistral_client", return_value=client):
        results = retrieve_narrative("query", con)

    assert results == []
