"""FHIR resource flattening for Bronze/Silver layers."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from .metadata import add_metadata, utc_now_iso


def _safe_get(d: Any, *path: str, default: Any = None) -> Any:
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur


def _coding_display(CodeableConcept: Any) -> tuple[str | None, str | None, str | None]:
    if not isinstance(CodeableConcept, dict):
        return None, None, None
    text = CodeableConcept.get("text")
    coding = (CodeableConcept.get("coding") or [None])[0] or {}
    return coding.get("system"), coding.get("code"), coding.get("display") or text


def flatten_patient(resource: dict[str, Any]) -> dict[str, Any]:
    name0 = (resource.get("name") or [{}])[0]
    given = " ".join(name0.get("given") or [])
    family = name0.get("family")
    telecom = (resource.get("telecom") or [{}])[0]
    address = (resource.get("address") or [{}])[0]
    return {
        "resource_id": resource.get("id"),
        "resource_type": "Patient",
        "resource_version_id": _safe_get(resource, "meta", "versionId"),
        "fhir_last_updated": _safe_get(resource, "meta", "lastUpdated"),
        "active": resource.get("active"),
        "gender": resource.get("gender"),
        "birth_date": resource.get("birthDate"),
        "family_name": family,
        "given_name": given,
        "full_name": " ".join(x for x in [given, family] if x).strip() or None,
        "phone": telecom.get("value") if telecom.get("system") == "phone" else telecom.get("value"),
        "city": address.get("city"),
        "state": address.get("state"),
        "country": address.get("country"),
        "postal_code": address.get("postalCode"),
    }


def flatten_encounter(resource: dict[str, Any]) -> dict[str, Any]:
    sys, code, disp = _coding_display((resource.get("type") or [None])[0])
    period = resource.get("period") or {}
    patient_ref = _safe_get(resource, "subject", "reference")
    return {
        "resource_id": resource.get("id"),
        "resource_type": "Encounter",
        "resource_version_id": _safe_get(resource, "meta", "versionId"),
        "fhir_last_updated": _safe_get(resource, "meta", "lastUpdated"),
        "status": resource.get("status"),
        "class_code": _safe_get(resource, "class", "code"),
        "type_system": sys,
        "type_code": code,
        "type_display": disp,
        "patient_id": (patient_ref or "").split("/")[-1] or None,
        "patient_reference": patient_ref,
        "period_start": period.get("start"),
        "period_end": period.get("end"),
    }


def flatten_observation(resource: dict[str, Any]) -> dict[str, Any]:
    sys, code, disp = _coding_display(resource.get("code"))
    patient_ref = _safe_get(resource, "subject", "reference")
    encounter_ref = _safe_get(resource, "encounter", "reference")
    value = None
    value_unit = None
    value_type = None
    if "valueQuantity" in resource:
        raw_val = _safe_get(resource, "valueQuantity", "value")
        value = str(raw_val) if raw_val is not None else None
        value_unit = _safe_get(resource, "valueQuantity", "unit")
        value_type = "Quantity"
    elif "valueString" in resource:
        value = str(resource.get("valueString")) if resource.get("valueString") is not None else None
        value_type = "String"
    elif "valueCodeableConcept" in resource:
        _, vcode, vdisp = _coding_display(resource.get("valueCodeableConcept"))
        value = vdisp or vcode
        value_type = "CodeableConcept"
    elif "valueBoolean" in resource:
        value = str(resource.get("valueBoolean"))
        value_type = "Boolean"
    elif "valueInteger" in resource:
        value = str(resource.get("valueInteger"))
        value_type = "Integer"

    return {
        "resource_id": resource.get("id"),
        "resource_type": "Observation",
        "resource_version_id": _safe_get(resource, "meta", "versionId"),
        "fhir_last_updated": _safe_get(resource, "meta", "lastUpdated"),
        "status": resource.get("status"),
        "code_system": sys,
        "code": code,
        "code_display": disp,
        "patient_id": (patient_ref or "").split("/")[-1] or None,
        "patient_reference": patient_ref,
        "encounter_id": (encounter_ref or "").split("/")[-1] or None,
        "encounter_reference": encounter_ref,
        "effective_datetime": resource.get("effectiveDateTime")
        or _safe_get(resource, "effectivePeriod", "start"),
        "value": value,
        "value_unit": value_unit,
        "value_type": value_type,
    }


def _first_coding_code(concept: Any) -> str | None:
    if not isinstance(concept, dict):
        return None
    coding = concept.get("coding") or []
    if not coding:
        return None
    return coding[0].get("code")


def flatten_condition(resource: dict[str, Any]) -> dict[str, Any]:
    sys, code, disp = _coding_display(resource.get("code"))
    patient_ref = _safe_get(resource, "subject", "reference")
    encounter_ref = _safe_get(resource, "encounter", "reference")
    onset = resource.get("onsetDateTime") or _safe_get(resource, "onsetPeriod", "start")
    return {
        "resource_id": resource.get("id"),
        "resource_type": "Condition",
        "resource_version_id": _safe_get(resource, "meta", "versionId"),
        "fhir_last_updated": _safe_get(resource, "meta", "lastUpdated"),
        "clinical_status": _first_coding_code(resource.get("clinicalStatus")),
        "verification_status": _first_coding_code(resource.get("verificationStatus")),
        "code_system": sys,
        "code": code,
        "code_display": disp,
        "patient_id": (patient_ref or "").split("/")[-1] or None,
        "patient_reference": patient_ref,
        "encounter_id": (encounter_ref or "").split("/")[-1] or None,
        "encounter_reference": encounter_ref,
        "onset_datetime": onset,
        "recorded_date": resource.get("recordedDate"),
    }


FLATTENERS = {
    "Patient": flatten_patient,
    "Encounter": flatten_encounter,
    "Observation": flatten_observation,
    "Condition": flatten_condition,
}


def entries_to_bronze_records(
    resource_name: str,
    entries: list[dict[str, Any]],
    *,
    api_url_or_params: str,
    api_call_timestamp: str,
) -> list[dict[str, Any]]:
    flatten = FLATTENERS[resource_name]
    extraction_ts = utc_now_iso()
    saved_ts = utc_now_iso()
    rows: list[dict[str, Any]] = []
    for entry in entries:
        resource = entry.get("resource") or entry
        if not resource.get("id"):
            continue
        flat = flatten(resource)
        flat["raw_json"] = json.dumps(resource, ensure_ascii=False, default=str)
        flat = add_metadata(
            flat,
            api_url_or_params=api_url_or_params,
            extraction_timestamp=extraction_ts,
            api_call_timestamp=api_call_timestamp,
            saved_timestamp=saved_ts,
        )
        rows.append(flat)
    return rows


def bronze_to_silver_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean + deduplicate for Silver (latest fhir_last_updated / extraction per id)."""
    if df.empty:
        return df.copy()
    out = df.copy()
    # Drop rows without primary id
    out = out[out["resource_id"].notna() & (out["resource_id"].astype(str) != "")]
    # Normalize strings
    for col in out.select_dtypes(include=["object", "string"]).columns:
        if col == "raw_json":
            continue
        out[col] = out[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    # Deduplicate: keep latest by fhir_last_updated then extraction_timestamp
    sort_cols = [c for c in ["fhir_last_updated", "extraction_timestamp"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=True)
    out = out.drop_duplicates(subset=["resource_id"], keep="last")
    return out.reset_index(drop=True)
