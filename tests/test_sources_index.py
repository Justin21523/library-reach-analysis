from __future__ import annotations

from pathlib import Path

from libraryreach.ingestion.sources_index import SourceRecord, load_sources_index, upsert_source_record


def test_sources_index_upsert(tmp_path: Path) -> None:
    settings = {
        "paths": {"raw_dir": str(tmp_path / "raw")},
    }

    r1 = SourceRecord(
        source_id="x",
        fetched_at="2026-01-01T00:00:00+00:00",
        output_path="data/raw/x.csv",
        checksum_sha256="abc",
        status="ok",
        details={"n": 1},
    )
    upsert_source_record(settings, r1)

    idx = load_sources_index(settings)
    assert isinstance(idx, dict)
    assert isinstance(idx.get("sources"), list)
    assert len(idx["sources"]) == 1
    assert idx["sources"][0]["source_id"] == "x"

    r2 = SourceRecord(
        source_id="x",
        fetched_at="2026-01-02T00:00:00+00:00",
        output_path="data/raw/x.csv",
        checksum_sha256="def",
        status="ok",
        details={"n": 2},
    )
    upsert_source_record(settings, r2)
    idx2 = load_sources_index(settings)
    assert len(idx2["sources"]) == 1
    assert idx2["sources"][0]["checksum_sha256"] == "def"

