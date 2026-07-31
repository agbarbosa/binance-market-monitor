from __future__ import annotations

import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_project_requires_python_312_everywhere() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["requires-python"] == ">=3.12"
    assert "Programming Language :: Python :: 3.11" not in pyproject["project"]["classifiers"]
    assert "Programming Language :: Python :: 3.12" in pyproject["project"]["classifiers"]
    assert pyproject["tool"]["ruff"]["target-version"] == "py312"
    assert pyproject["tool"]["mypy"]["python_version"] == "3.12"

    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.12-slim AS runtime" in dockerfile
    assert "python:3.11" not in dockerfile


def test_compose_runs_functional_monitor_on_host_loopback_with_persistent_data() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    service = compose["services"]["binance-market-monitor"]

    # Host networking lets the process keep its strict 127.0.0.1 bind while
    # making that loopback API reachable by the host-side Hermes plugin.
    assert service["network_mode"] == "host"
    assert "ports" not in service
    assert service["restart"] == "unless-stopped"
    assert "./data:/app/data" in service["volumes"]
    assert service["command"][:4] == [
        "uv",
        "run",
        "binance-market-monitor",
        "monitor",
    ]
    assert "uvicorn" not in service["command"]
    assert "--serve-api" in service["command"]
