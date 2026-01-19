import pandas as pd

from libraryreach.api.summary import parse_bbox, score_buckets, score_histogram, summarize


def test_parse_bbox_roundtrip():
    bbox = parse_bbox("120.0,23.5,122.0,25.5")
    assert bbox is not None
    assert bbox.min_lon == 120.0
    assert bbox.min_lat == 23.5
    assert bbox.max_lon == 122.0
    assert bbox.max_lat == 25.5


def test_score_buckets_and_histogram():
    s = pd.Series([10, 39.9, 40, 69.9, 70, 100])
    assert score_buckets(s) == {"low": 2, "mid": 2, "high": 2}
    h = score_histogram(s, bins=[0, 50, 100])
    assert h["bins"] == [0, 50, 100]
    assert h["counts"] == [3, 3]


def test_summarize_filters_by_city():
    libs = pd.DataFrame(
        [
            {"id": "a", "city": "X", "accessibility_score": 80},
            {"id": "b", "city": "Y", "accessibility_score": 20},
        ]
    )
    deserts = pd.DataFrame(
        [
            {"cell_id": "c1", "city": "X", "is_desert": True},
            {"cell_id": "c2", "city": "Y", "is_desert": True},
        ]
    )
    outreach = pd.DataFrame(
        [
            {"id": "o1", "city": "X", "outreach_score": 9.5},
            {"id": "o2", "city": "Y", "outreach_score": 1.0},
        ]
    )
    out = summarize(libraries=libs, deserts=deserts, outreach=outreach, cities=["X"], top_n_outreach=10)
    assert out["metrics"]["libraries_count"] == 1
    assert out["metrics"]["deserts_count"] == 1
    assert out["metrics"]["outreach_count"] == 1
    assert out["metrics"]["score_buckets"]["high"] == 1
    assert "deserts_distributions" in out
    assert "outreach_distributions" in out
