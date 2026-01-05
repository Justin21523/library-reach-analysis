import pandas as pd

from libraryreach.catalogs.validators import (
    validate_libraries_catalog,
    validate_outreach_candidates_catalog,
)


def test_validate_libraries_unknown_city_fails() -> None:
    libraries = pd.DataFrame(
        [
            {
                "id": "X-001",
                "name": "Test Library",
                "address": "Test Address",
                "lat": 25.0,
                "lon": 121.0,
                "city": "UnknownCity",
                "district": "Test",
            }
        ]
    )
    result = validate_libraries_catalog(libraries, allowed_cities={"Taipei"})
    assert not result.ok
    assert any("unknown city values" in e for e in result.errors)


def test_validate_outreach_unknown_type_fails() -> None:
    outreach = pd.DataFrame(
        [
            {
                "id": "C-001",
                "name": "Test Candidate",
                "type": "invalid_type",
                "address": "Test Address",
                "lat": 25.0,
                "lon": 121.0,
                "city": "Taipei",
                "district": "Test",
            }
        ]
    )
    result = validate_outreach_candidates_catalog(
        outreach,
        allowed_cities={"Taipei"},
        allowed_types={"community_center"},
    )
    assert not result.ok
    assert any("unknown type values" in e for e in result.errors)

