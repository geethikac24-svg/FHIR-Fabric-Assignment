import json
from pathlib import Path

import pandas as pd

from fhir_lakehouse.scd2 import apply_scd2, compute_version_hash
from fhir_lakehouse.transforms import flatten_patient, bronze_to_silver_df


def test_version_hash_stable():
    a = {"resource_id": "1", "gender": "female", "extraction_timestamp": "t1"}
    b = {"resource_id": "1", "gender": "female", "extraction_timestamp": "t2"}
    exclude = ["extraction_timestamp"]
    assert compute_version_hash(a, exclude) == compute_version_hash(b, exclude)


def test_scd2_insert_and_change():
    incoming1 = pd.DataFrame(
        [{"resource_id": "p1", "gender": "male", "version_hash": "h1"}]
    )
    hist = apply_scd2(pd.DataFrame(), incoming1, primary_key="resource_id")
    assert len(hist) == 1
    assert bool(hist.iloc[0]["is_current"]) is True

    incoming2 = pd.DataFrame(
        [{"resource_id": "p1", "gender": "female", "version_hash": "h2"}]
    )
    hist2 = apply_scd2(hist, incoming2, primary_key="resource_id")
    assert len(hist2) == 2
    assert hist2["is_current"].sum() == 1
    current = hist2[hist2["is_current"] == True].iloc[0]  # noqa: E712
    assert current["gender"] == "female"


def test_flatten_patient_minimal():
    resource = {
        "id": "123",
        "resourceType": "Patient",
        "gender": "female",
        "birthDate": "1990-01-01",
        "meta": {"lastUpdated": "2026-08-01T00:00:00Z", "versionId": "1"},
        "name": [{"family": "Doe", "given": ["Jane"]}],
    }
    row = flatten_patient(resource)
    assert row["resource_id"] == "123"
    assert row["full_name"] == "Jane Doe"


def test_bronze_dedupe():
    df = pd.DataFrame(
        [
            {
                "resource_id": "1",
                "fhir_last_updated": "2026-08-01T00:00:00Z",
                "extraction_timestamp": "a",
                "gender": "male",
            },
            {
                "resource_id": "1",
                "fhir_last_updated": "2026-08-02T00:00:00Z",
                "extraction_timestamp": "b",
                "gender": "female",
            },
        ]
    )
    out = bronze_to_silver_df(df)
    assert len(out) == 1
    assert out.iloc[0]["gender"] == "female"


def test_settings_loads():
    from fhir_lakehouse.config import load_settings, get_resources_ordered

    cfg = load_settings()
    names = [r["name"] for r in get_resources_ordered(cfg)]
    assert names == ["Patient", "Encounter", "Observation", "Condition"]
