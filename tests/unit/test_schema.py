from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

import pytest

from pr_compliance.exceptions import SchemaValidationError
from pr_compliance.transform.schema_loader import ENTITY_SCHEMAS, load_schema


@pytest.mark.parametrize("entity", list(ENTITY_SCHEMAS))
def test_schemas_load(entity: str) -> None:
    assert load_schema(entity).schema["$schema"].endswith("2020-12/schema")


def test_real_payloads_are_valid(fixture: Callable[[str], Any]) -> None:
    pr = next(p for p in fixture("pulls_page.json") if p["merged_at"])
    assert load_schema("pull_requests").errors(pr) == []
    for r in fixture("reviews_699.json"):
        assert load_schema("reviews").errors(r) == []
    for c in fixture("commits_699.json"):
        assert load_schema("commits").errors(c) == []
    assert load_schema("statuses").errors(fixture("status_699.json")) == []
    for cr in fixture("check_runs_699.json")["check_runs"]:
        assert load_schema("check_runs").errors(cr) == []


def test_missing_required_field_reported(fixture: Callable[[str], Any]) -> None:
    pr = copy.deepcopy(fixture("pulls_page.json")[0])
    del pr["title"]
    errors = load_schema("pull_requests").errors(pr)
    assert any("title" in e for e in errors)


def test_wrong_type_and_bad_enum(fixture: Callable[[str], Any]) -> None:
    review = copy.deepcopy(fixture("reviews_699.json")[0])
    review["state"] = "LGTM"
    review["id"] = "not-an-int"
    errors = load_schema("reviews").errors(review)
    assert len(errors) == 2


def test_bad_timestamp_rejected(fixture: Callable[[str], Any]) -> None:
    pr = copy.deepcopy(fixture("pulls_page.json")[0])
    pr["merged_at"] = "yesterday"
    assert load_schema("pull_requests").errors(pr)


def test_extra_fields_allowed(fixture: Callable[[str], Any]) -> None:
    pr = copy.deepcopy(fixture("pulls_page.json")[0])
    pr["brand_new_github_field"] = {"x": 1}
    assert load_schema("pull_requests").errors(pr) == []


def test_null_user_allowed(fixture: Callable[[str], Any]) -> None:
    pr = copy.deepcopy(fixture("pulls_page.json")[0])
    pr["user"] = None
    assert load_schema("pull_requests").errors(pr) == []


def test_dtypes_from_schema() -> None:
    s = load_schema("pull_requests")
    assert s.dtype_for("number") == "Int64"
    assert s.dtype_for("user.login") == "string"
    assert s.dtype_for("merged_at") == "datetime64[ns, UTC]"
    assert load_schema("statuses").dtype_for("statuses.context") == "string"
    with pytest.raises(SchemaValidationError):
        s.dtype_for("nope")
