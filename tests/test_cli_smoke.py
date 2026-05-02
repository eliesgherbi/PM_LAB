from __future__ import annotations

from typer.testing import CliRunner

from polymarket_insight.cli import app


def test_cli_version():
    result = CliRunner().invoke(app, ["version"])

    assert result.exit_code == 0
    assert "polymarket-insight" in result.stdout
