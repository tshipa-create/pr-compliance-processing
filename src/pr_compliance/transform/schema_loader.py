"""Load the JSON Schemas that predefine the raw input, and derive Pandas dtypes from them.

Each raw entity has a Draft 2020-12 schema under ``pr_compliance/schemas``. The same
schema is used twice: to validate every raw record (bad ones go to quarantine), and to
decide the dtype of every silver column, so field types are declared in one place.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from importlib import resources
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from pr_compliance.exceptions import SchemaValidationError

SCHEMA_PACKAGE = "pr_compliance.schemas"
ENTITY_SCHEMAS = {
    "pull_requests": "pull_request.schema.json",
    "reviews": "review.schema.json",
    "commits": "commit.schema.json",
    "statuses": "combined_status.schema.json",
    "check_runs": "check_run.schema.json",
}
BASE_URI = "https://pr-compliance.local/schemas/"

_format_checker = FormatChecker()


@_format_checker.checks("date-time", raises=ValueError)
def _is_datetime(value: object) -> bool:
    """RFC 3339 check without the optional ``rfc3339-validator`` dependency."""
    if not isinstance(value, str):
        return True
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return "T" in value


@dataclass(frozen=True)
class EntitySchema:
    entity: str
    schema: dict[str, Any]
    validator: Draft202012Validator
    registry: Registry[Any]

    def errors(self, record: Any) -> list[str]:
        """Return human-readable validation errors (``path: message``), empty if valid."""
        return [
            f"{'/'.join(str(p) for p in err.absolute_path) or '<root>'}: {err.message}"
            for err in sorted(self.validator.iter_errors(record), key=lambda e: list(e.path))
        ]

    def dtype_for(self, dotted_path: str) -> str:
        """Pandas dtype for a (possibly nested) field, e.g. ``user.login`` -> ``string``."""
        node = self.schema
        for part in dotted_path.split("."):
            node = self._resolve(node)
            if "items" in node:  # step through arrays: statuses.context -> items.context
                node = self._resolve(node["items"])
            props = node.get("properties", {})
            if part not in props:
                raise SchemaValidationError(f"{self.entity}: field {dotted_path!r} not in schema")
            node = props[part]
        return _json_type_to_dtype(self._resolve(node))

    def _resolve(self, node: dict[str, Any]) -> dict[str, Any]:
        while "$ref" in node:
            ref = node["$ref"]
            resolved = self.registry.resolver(base_uri=BASE_URI).lookup(ref)
            node = resolved.contents
        return node


def _json_type_to_dtype(node: dict[str, Any]) -> str:
    types = node.get("type", "string")
    if isinstance(types, list):
        types = next((t for t in types if t != "null"), "string")
    if "const" in node and isinstance(node["const"], str):
        types = "string"
    if types == "integer":
        return "Int64"
    if types == "number":
        return "Float64"
    if types == "boolean":
        return "boolean"
    if types == "string" and node.get("format") == "date-time":
        return "datetime64[ns, UTC]"
    if types == "string":
        return "string"
    return "object"


def _read_schema(name: str) -> dict[str, Any]:
    try:
        text = resources.files(SCHEMA_PACKAGE).joinpath(name).read_text(encoding="utf-8")
        schema: dict[str, Any] = json.loads(text)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise SchemaValidationError(f"cannot load schema {name}: {exc}") from exc
    return schema


@cache
def _registry() -> Registry[Any]:
    resources_: list[tuple[str, Resource[Any]]] = []
    for name in ["_defs.schema.json", *ENTITY_SCHEMAS.values()]:
        schema = _read_schema(name)
        resources_.append((schema["$id"], DRAFT202012.create_resource(schema)))
    return Registry().with_resources(resources_)


@cache
def load_schema(entity: str) -> EntitySchema:
    """Return the compiled schema for a bronze entity name (``pull_requests``...)."""
    if entity not in ENTITY_SCHEMAS:
        raise SchemaValidationError(f"no schema for entity {entity!r}")
    schema = _read_schema(ENTITY_SCHEMAS[entity])
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise SchemaValidationError(f"invalid schema for {entity}: {exc.message}") from exc
    registry = _registry()
    validator = Draft202012Validator(schema, registry=registry, format_checker=_format_checker)
    return EntitySchema(entity=entity, schema=schema, validator=validator, registry=registry)
