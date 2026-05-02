"""Typed configuration for Polymarket Insight."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


class DataConfig(BaseModel):
    """Filesystem locations for generated research data."""

    root_dir: Path = Path("data")
    raw_dir: Path = Path("data/raw")
    normalized_dir: Path = Path("data/normalized")
    marts_dir: Path = Path("data/marts")
    cache_dir: Path = Path("data/cache")
    manifest_path: Path = Path("data/manifest.duckdb")
    logs_dir: Path = Path("data/logs")


class ApiConfig(BaseModel):
    """Public Polymarket API settings."""

    gamma_base_url: str = "https://gamma-api.polymarket.com"
    data_api_base_url: str = "https://data-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    timeout_s: float = 20.0
    max_retries: int = 3
    backoff_base_s: float = 0.5
    max_concurrency: int = 8


class CacheConfig(BaseModel):
    """Short-lived HTTP cache TTLs."""

    default_ttl_s: int = 300
    market_ttl_s: int = 3600
    leaderboard_ttl_s: int = 3600
    orderbook_ttl_s: int = 30
    price_history_ttl_s: int = 3600


class IngestionConfig(BaseModel):
    """Ingestion job behavior."""

    liquid_market_min_24h_volume_usd: Decimal = Decimal("5000")
    include_markets_resolving_within_days: int = 7
    orderbook_snapshot_interval_s: int = 300
    orderbook_max_concurrency: int = 8
    orderbook_per_market_timeout_s: int = 15


class LoggingConfig(BaseModel):
    """Logging output settings."""

    model_config = {"populate_by_name": True}

    level: str = "INFO"
    json_logs: bool = Field(default=True, alias="json")


class Settings(BaseSettings):
    """Application settings.

    Environment variables can override nested values with the `PMI__` prefix,
    for example `PMI__DATA__ROOT_DIR=tmp/data`.
    """

    model_config = SettingsConfigDict(
        env_prefix="PMI__",
        env_nested_delimiter="__",
        extra="ignore",
    )

    data: DataConfig = Field(default_factory=DataConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def _merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Load settings from defaults, optional TOML, and environment variables."""

    data: dict[str, Any] = {}
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file does not exist: {path}")
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    return Settings(**data)


def resolve_project_path(path: str | Path) -> Path:
    """Resolve relative runtime paths from the nearest project root.

    Notebook kernels often start in `notebooks/`, while CLI commands usually run
    from the repository root. This keeps default `data/...` paths consistent.
    """

    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    for parent in [Path.cwd(), *Path.cwd().parents]:
        if (parent / "pyproject.toml").exists() and (parent / "src").exists():
            return parent / candidate
    return candidate


def ensure_data_dirs(settings: Settings) -> None:
    """Create runtime data directories if needed."""

    for path in (
        settings.data.root_dir,
        settings.data.raw_dir,
        settings.data.normalized_dir,
        settings.data.marts_dir,
        settings.data.cache_dir,
        settings.data.logs_dir,
        settings.data.manifest_path.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)
