from pathlib import Path

import pandas as pd

from libraryreach.catalogs.load import load_libraries_catalog, load_outreach_candidates_catalog


def _write_csv(path: Path, text: str) -> None:
    path.write_text(text.strip() + "\n", encoding="utf-8")


def test_load_catalogs_normalizes_city_and_type(tmp_path: Path) -> None:
    catalogs_dir = tmp_path / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        catalogs_dir / "libraries.csv",
        """
id,name,address,latitude,longitude,city,district
L-001,Test Library,Addr,25.0,121.0,臺北市,TestDistrict
""",
    )
    _write_csv(
        catalogs_dir / "outreach_candidates.csv",
        """
id,name,type,address,lat,lng,city,district
C-001,Test Site,Community Center,Addr,25.0,121.0,台北市,TestDistrict
""",
    )

    settings = {
        "paths": {"catalogs_dir": str(catalogs_dir)},
        "aoi": {"city_aliases": {"臺北市": "Taipei", "台北市": "Taipei"}},
    }

    libraries = load_libraries_catalog(settings)
    outreach = load_outreach_candidates_catalog(settings)

    assert libraries.loc[0, "city"] == "Taipei"
    assert isinstance(libraries.loc[0, "lat"], float)
    assert isinstance(libraries.loc[0, "lon"], float)

    assert outreach.loc[0, "city"] == "Taipei"
    assert outreach.loc[0, "type"] == "community_center"

