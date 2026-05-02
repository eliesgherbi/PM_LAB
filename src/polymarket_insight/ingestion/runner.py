"""Shared ingestion setup."""

from __future__ import annotations

from polymarket_insight.config import Settings, ensure_data_dirs, load_settings
from polymarket_insight.data.clients.base import PublicHttpClient
from polymarket_insight.data.clients.clob import ClobClient
from polymarket_insight.data.clients.data_api import DataApiClient
from polymarket_insight.data.clients.gamma import GammaClient
from polymarket_insight.data.storage.manifest import Manifest
from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.data.storage.raw import RawStore


class Runtime:
    """Runtime objects shared by CLI and ingestion jobs."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()
        ensure_data_dirs(self.settings)
        self.raw_store = RawStore(self.settings.data.raw_dir)
        self.normalized_store = NormalizedStore(self.settings.data.normalized_dir)
        self.manifest = Manifest(self.settings.data.manifest_path)
        self.gamma = GammaClient(
            PublicHttpClient(
                source="gamma",
                base_url=self.settings.api.gamma_base_url,
                timeout_s=self.settings.api.timeout_s,
                max_retries=self.settings.api.max_retries,
                backoff_base_s=self.settings.api.backoff_base_s,
                raw_store=self.raw_store,
            )
        )
        self.data_api = DataApiClient(
            PublicHttpClient(
                source="data_api",
                base_url=self.settings.api.data_api_base_url,
                timeout_s=self.settings.api.timeout_s,
                max_retries=self.settings.api.max_retries,
                backoff_base_s=self.settings.api.backoff_base_s,
                raw_store=self.raw_store,
            )
        )
        self.clob = ClobClient(
            PublicHttpClient(
                source="clob",
                base_url=self.settings.api.clob_base_url,
                timeout_s=self.settings.api.timeout_s,
                max_retries=self.settings.api.max_retries,
                backoff_base_s=self.settings.api.backoff_base_s,
                raw_store=self.raw_store,
            )
        )
