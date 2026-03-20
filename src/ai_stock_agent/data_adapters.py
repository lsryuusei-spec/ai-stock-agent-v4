from __future__ import annotations

import json
from pathlib import Path

from .models import AdapterPayload, DataProviderConfig, EntityProfile, TriggerEventRecord
from .providers import make_provider


DEFAULT_PROVIDER_CONFIGS = [
    DataProviderConfig(
        provider_id="macro_primary",
        data_domain="macro_prices",
        mode="primary",
        priority=1,
        provider_kind="mock",
        latency_hours=2.0,
        field_completeness=0.96,
    ),
    DataProviderConfig(
        provider_id="macro_backup",
        data_domain="macro_prices",
        mode="fallback",
        priority=2,
        provider_kind="mock",
        latency_hours=5.0,
        field_completeness=0.82,
    ),
    DataProviderConfig(
        provider_id="market_internal",
        data_domain="market_breadth",
        mode="primary",
        priority=1,
        provider_kind="mock",
        latency_hours=1.0,
        field_completeness=0.95,
    ),
    DataProviderConfig(
        provider_id="sector_internal",
        data_domain="sector_signals",
        mode="primary",
        priority=1,
        provider_kind="mock",
        latency_hours=4.0,
        field_completeness=0.91,
    ),
    DataProviderConfig(
        provider_id="filing_primary",
        data_domain="filing_metrics",
        mode="primary",
        priority=1,
        provider_kind="mock",
        latency_hours=6.0,
        field_completeness=0.93,
    ),
    DataProviderConfig(
        provider_id="filing_backup",
        data_domain="filing_metrics",
        mode="fallback",
        priority=2,
        provider_kind="mock",
        latency_hours=12.0,
        field_completeness=0.78,
    ),
]


def load_provider_configs(config_path: str | None = None) -> list[DataProviderConfig]:
    if not config_path:
        return list(DEFAULT_PROVIDER_CONFIGS)
    path = Path(config_path)
    if not path.exists():
        return list(DEFAULT_PROVIDER_CONFIGS)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [DataProviderConfig.model_validate(item) for item in payload]


class DataSourceManager:
    def __init__(self, provider_configs: list[DataProviderConfig]):
        self.provider_configs = provider_configs

    def _find_configs(self, data_domain: str, mode: str | None = None) -> list[DataProviderConfig]:
        configs = [config for config in self.provider_configs if config.data_domain == data_domain and config.enabled]
        if mode is not None:
            configs = [config for config in configs if config.mode == mode]
        return sorted(configs, key=lambda item: item.priority)

    def _fetch(
        self,
        data_domain: str,
        *,
        trigger_events: list[TriggerEventRecord] | None = None,
        entity: EntityProfile | None = None,
        params: dict | None = None,
    ) -> AdapterPayload:
        primary = self._find_configs(data_domain, "primary")
        fallback = self._find_configs(data_domain, "fallback")
        ordered = primary + fallback if primary else fallback
        if not ordered:
            raise ValueError(f"No provider configured for data_domain={data_domain}")
        last_payload: AdapterPayload | None = None
        for config in ordered:
            provider = make_provider(config)
            payload = provider.fetch(trigger_events=trigger_events, entity=entity, params=params)
            last_payload = payload
            if not payload.degraded and payload.missing_reason is None:
                return payload
        return last_payload

    def get_macro_prices(self, trigger_events: list[TriggerEventRecord]) -> AdapterPayload:
        return self._fetch("macro_prices", trigger_events=trigger_events)

    def get_market_breadth(self, trigger_events: list[TriggerEventRecord]) -> AdapterPayload:
        return self._fetch("market_breadth", trigger_events=trigger_events)

    def get_sector_signals(self, trigger_events: list[TriggerEventRecord]) -> AdapterPayload:
        return self._fetch("sector_signals", trigger_events=trigger_events)

    def get_entity_filing_metrics(self, entity: EntityProfile) -> AdapterPayload:
        return self._fetch("filing_metrics", entity=entity)


def build_data_source_manager(config_path: str | None = None) -> DataSourceManager:
    return DataSourceManager(load_provider_configs(config_path))
