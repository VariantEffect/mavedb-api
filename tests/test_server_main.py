# ruff: noqa: E402

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")


def test_served_openapi_schema_keeps_its_customizations(client):
    """FastAPI rebuilds its own schema once the routes change, as they do when the MCP server is mounted."""
    schema = client.get("/openapi.json").json()

    assert schema["info"]["title"] == "MaveDB API"
    assert {"Experiments", "Score Sets"} <= {tag["name"] for tag in schema["tags"]}
    assert "ScoreCalibrationModify" in schema["components"]["schemas"]
