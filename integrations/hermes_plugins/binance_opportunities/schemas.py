"""JSON schema for the Binance opportunities Hermes tool."""

from __future__ import annotations

BINANCE_OPPORTUNITIES_LIST_SCHEMA = {
    "name": "binance_opportunities_list",
    "description": (
        "List current Binance Market Monitor opportunities by reading only the local "
        "read-only API at http://127.0.0.1:8000. Use this whenever the user asks "
        "to list Binance coins, watches, candidates, or opportunities. The tool "
        "combines Stage1 candidates with recent watch alerts, explains reason codes "
        "and features, and sorts confirmed watches first. Never uses credentials, "
        "trading endpoints, external market APIs, or Open Interest. Output is not "
        "financial advice and contains no trade recommendation."
    ),
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "default": 10,
                "description": "Maximum number of opportunities to return, from 1 to 50.",
            },
            "min_stage1_score": {
                "type": "number",
                "default": 0,
                "description": (
                    "Minimum Stage1 candidate score. Alert-only confirmed watches are kept "
                    "only when this threshold is zero because they have no current Stage1 score."
                ),
            },
            "min_watch_score": {
                "type": "integer",
                "minimum": 0,
                "maximum": 100,
                "default": 0,
                "description": (
                    "Minimum watch alert score from 0 to 100. Values above zero naturally "
                    "exclude unconfirmed Stage1-only candidates."
                ),
            },
            "confirmed_only": {
                "type": "boolean",
                "default": False,
                "description": (
                    "When true, return only symbols with a recent watch alert. This includes "
                    "alert-only symbols even if they are no longer in current Stage1 candidates."
                ),
            },
        },
    },
}
