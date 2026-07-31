from __future__ import annotations

import json
import re
from pathlib import Path

from binance_market_monitor.contracts.models import ContractBase, contract_models

JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"


def write_json_schemas(output_dir: str | Path) -> list[Path]:
    """Write one versioned JSON Schema file for every PRD contract model."""

    schema_dir = Path(output_dir)
    schema_dir.mkdir(parents=True, exist_ok=True)

    written_paths: list[Path] = []
    for model in contract_models():
        schema = model.model_json_schema(mode="validation")
        schema["$schema"] = JSON_SCHEMA_DRAFT
        schema["x-schema-version"] = ContractBase().schema_version
        schema_path = schema_dir / f"{_snake_case(model.__name__)}.schema.v1.json"
        schema_path.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written_paths.append(schema_path)

    return written_paths


def _snake_case(name: str) -> str:
    with_initialisms = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", with_initialisms).lower()
