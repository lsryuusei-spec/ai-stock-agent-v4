from __future__ import annotations

from datetime import UTC, date, datetime
import os
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from ai_stock_agent.models import Bucket, DataProviderConfig, EntityProfile
from ai_stock_agent.data_adapters import DataSourceManager
from ai_stock_agent.dashboard import (
    bootstrap_demo_action,
    build_dashboard_snapshot,
    build_universe_action,
    execute_manual_override_action,
    ingest_knowledge_batch,
    prepare_notebooklm_batch,
    refresh_knowledge_state,
)
from ai_stock_agent.knowledge_base import (
    build_topic_version_diff,
    build_knowledge_context,
    default_knowledge_policy,
    ingest_knowledge_payload,
    load_knowledge_document_payloads,
    prepare_notebooklm_documents,
    query_knowledge_slices,
    refresh_slice_statuses,
)
from ai_stock_agent.evidence_fusion import build_entity_evidence_summary
from ai_stock_agent.providers import make_provider
from ai_stock_agent.scoring import build_frozen_packet, compute_scorecard, validate_bucket_transition
from ai_stock_agent.storage import SQLiteStateStore
from ai_stock_agent.tushare_probe import probe_tushare_access
from ai_stock_agent.universe_builder import build_initial_universe_bundle
from ai_stock_agent.workflow import apply_manual_override, bootstrap_demo_data, run_mvp_workflow
from ai_stock_agent.web_research import build_web_research_manager, synthesize_trigger_events


class WorkflowTestCase(unittest.TestCase):
    def test_periodic_review_runs_and_merges(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(db_path=db_path, run_mode="periodic_review")
            self.assertEqual(result["run_state"].run_status.value, "merged")
            self.assertTrue(result["pool"].current_pool_members)
            self.assertIn("theme_decomposition", result)
            self.assertIn("market_context", result)
            self.assertIn("pool_snapshot", result)
            self.assertIn("archive_record", result)
            self.assertIn("evaluation_metrics", result)
            self.assertIn("post_mortem_report", result)
            self.assertIn("evidence_summaries", result)
            self.assertTrue(result["theme_decomposition"].theme_slices)
            self.assertTrue(result["market_context"].indicators)
            self.assertTrue(result["evidence_summaries"])

    def test_event_driven_run_creates_trigger_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            run_mvp_workflow(
                db_path=db_path,
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "earnings", "ticker": "AMD"}],
            )
            store = SQLiteStateStore(db_path)
            self.assertGreaterEqual(len(store.list_trigger_events()), 1)
            self.assertGreaterEqual(len(store.list_market_contexts()), 1)
            self.assertGreaterEqual(len(store.list_theme_decompositions()), 1)
            self.assertGreaterEqual(len(store.list_prescreen_decisions()), 1)

    def test_manual_override_writes_audit_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            record = apply_manual_override(
                db_path=db_path,
                entity_id="tsm_us",
                override_field="thesis_status",
                new_value="fragile",
                reason="manual sanity check",
            )
            store = SQLiteStateStore(db_path)
            company = store.load_company("tsm_us")
            audits = store.list_audit_records()
            self.assertEqual(record.override_field, "thesis_status")
            self.assertEqual(company.thesis_status.value, "fragile")
            self.assertGreaterEqual(len(audits), 1)

    def test_scoring_notes_include_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "macro", "ticker": "AMD"}],
            )
            notes = []
            for delta in result["run_state"].decision_output["deltas"]:
                notes.extend(delta["score_snapshot"]["notes"])
            self.assertTrue(any(note.startswith("regime=") for note in notes))
            self.assertTrue(any(note.startswith("context_status=") for note in notes))
            self.assertTrue(any(note.startswith("advance_ratio=") for note in notes))
            self.assertTrue(any(note.startswith("breadth_thrust=") for note in notes))
            self.assertTrue(any(note.startswith("evidence_confidence=") for note in notes))
            self.assertTrue(any(note.startswith("evidence_label=") for note in notes))
            self.assertTrue(any(note.startswith("official_evidence=") for note in notes))

    def test_prescreen_and_factor_registry_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            run_mvp_workflow(db_path=db_path, run_mode="periodic_review")
            store = SQLiteStateStore(db_path)
            factors = store.list_factor_registry_items()
            prescreens = store.list_prescreen_decisions()
            self.assertGreaterEqual(len(factors), 1)
            self.assertGreaterEqual(len(prescreens), 1)
            self.assertTrue(any(not decision.passed for decision in prescreens))

    def test_pool_snapshots_and_archive_history_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            run_mvp_workflow(db_path=db_path, run_mode="periodic_review")
            store = SQLiteStateStore(db_path)
            snapshots = store.list_pool_snapshots()
            folds = store.list_company_history_folds()
            archives = store.list_archive_version_records()
            self.assertGreaterEqual(len(snapshots), 1)
            self.assertGreaterEqual(len(folds), 1)
            self.assertGreaterEqual(len(archives), 1)
            self.assertGreaterEqual(snapshots[0].version_number, 1)

    def test_postmortem_records_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            run_mvp_workflow(db_path=db_path, run_mode="periodic_review")
            store = SQLiteStateStore(db_path)
            metrics = store.list_evaluation_metrics()
            reports = store.list_post_mortem_reports()
            self.assertGreaterEqual(len(metrics), 1)
            self.assertGreaterEqual(len(reports), 1)
            self.assertIn("entry_hit_rate", metrics[0].model_dump())
            self.assertTrue(reports[0].summary)

    def test_custom_data_config_can_force_degraded_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            config_path = Path(tmpdir) / "data_sources.json"
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "macro_primary",
                            "data_domain": "macro_prices",
                            "mode": "primary",
                            "priority": 1,
                            "enabled": True,
                            "latency_hours": 2.0,
                            "field_completeness": 0.7,
                        },
                        {
                            "provider_id": "macro_backup",
                            "data_domain": "macro_prices",
                            "mode": "fallback",
                            "priority": 2,
                            "enabled": True,
                            "latency_hours": 5.0,
                            "field_completeness": 0.82,
                        },
                        {
                            "provider_id": "market_internal",
                            "data_domain": "market_breadth",
                            "mode": "primary",
                            "priority": 1,
                            "enabled": True,
                            "latency_hours": 1.0,
                            "field_completeness": 0.95,
                        },
                        {
                            "provider_id": "sector_internal",
                            "data_domain": "sector_signals",
                            "mode": "primary",
                            "priority": 1,
                            "enabled": True,
                            "latency_hours": 4.0,
                            "field_completeness": 0.83,
                        },
                        {
                            "provider_id": "filing_primary",
                            "data_domain": "filing_metrics",
                            "mode": "primary",
                            "priority": 1,
                            "enabled": True,
                            "latency_hours": 6.0,
                            "field_completeness": 0.7,
                        },
                        {
                            "provider_id": "filing_backup",
                            "data_domain": "filing_metrics",
                            "mode": "fallback",
                            "priority": 2,
                            "enabled": True,
                            "latency_hours": 12.0,
                            "field_completeness": 0.78,
                        },
                    ]
                ),
                encoding="utf-8",
            )
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                data_config_path=str(config_path),
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "macro", "ticker": "AMD"}],
            )
            self.assertEqual(result["market_context"].context_status, "degraded")

    def test_file_provider_config_can_drive_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            snapshot_path = Path(tmpdir) / "snapshot.json"
            config_path = Path(tmpdir) / "providers.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "macro_prices": {"ust_10y": 4.55, "volatility_regime": 24.5},
                        "market_breadth": {"liquidity_breadth": 49.0},
                        "sector_signals": {"ai_capex_momentum": 63.0},
                        "filing_metrics": {
                            "amd_us": {
                                "base_quality": 82,
                                "industry_position_score": 84,
                                "macro_alignment_score": 88,
                                "valuation_score": 70,
                                "catalyst_score": 78,
                                "risk_penalty": 38,
                                "composite_tradability": 91,
                                "data_freshness_hours": 108,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "macro_file",
                            "data_domain": "macro_prices",
                            "mode": "primary",
                            "provider_kind": "file",
                            "priority": 1,
                            "file_path": str(snapshot_path),
                            "field_completeness": 0.95,
                        },
                        {
                            "provider_id": "breadth_file",
                            "data_domain": "market_breadth",
                            "mode": "primary",
                            "provider_kind": "file",
                            "priority": 1,
                            "file_path": str(snapshot_path),
                            "field_completeness": 0.95,
                        },
                        {
                            "provider_id": "sector_file",
                            "data_domain": "sector_signals",
                            "mode": "primary",
                            "provider_kind": "file",
                            "priority": 1,
                            "file_path": str(snapshot_path),
                            "field_completeness": 0.95,
                        },
                        {
                            "provider_id": "filing_file",
                            "data_domain": "filing_metrics",
                            "mode": "primary",
                            "provider_kind": "file",
                            "priority": 1,
                            "file_path": str(snapshot_path),
                            "field_completeness": 0.95,
                        },
                    ]
                ),
                encoding="utf-8",
            )
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                data_config_path=str(config_path),
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "macro", "ticker": "AMD"}],
            )
            indicator_map = {item.label: item.value for item in result["market_context"].indicators}
            self.assertEqual(indicator_map["UST 10Y"], 4.55)
            self.assertEqual(indicator_map["Liquidity breadth"], 49.0)

    def test_web_research_records_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                run_mode="periodic_review",
            )
            store = SQLiteStateStore(db_path)
            records = store.list_web_research_records()
            self.assertTrue(result["web_research_records"])
            self.assertGreaterEqual(len(records), 1)

    def test_evidence_fusion_summaries_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "news", "ticker": "AMD"}],
                web_config_path="config/web_research.file.json",
            )
            store = SQLiteStateStore(db_path)
            summaries = store.list_evidence_fusion_summaries()
            self.assertTrue(result["evidence_summaries"])
            self.assertGreaterEqual(len(summaries), 2)
            self.assertTrue(any(summary.scope == "market" for summary in summaries))
            self.assertTrue(any(summary.entity_id is not None for summary in summaries))

    def test_bootstrap_demo_data_includes_cn_and_hk_universes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            store = SQLiteStateStore(db_path)
            universe_ids = {item.universe_id for item in store.list_universes()}
            pool_ids = {item.pool_id for item in store.list_pools()}
            self.assertIn("cn_macro_ai", universe_ids)
            self.assertIn("hk_macro_ai", universe_ids)
            self.assertIn("cn_macro_ai_pool", pool_ids)
            self.assertIn("hk_macro_ai_pool", pool_ids)

    def test_cn_workflow_can_run_on_cn_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                universe_id="cn_macro_ai",
                pool_id="cn_macro_ai_pool",
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "news", "ticker": "688981.SH"}],
                web_config_path="config/web_research.free_ah.json",
                data_config_path="config/data_sources.free_ah.json",
            )
            self.assertEqual(result["run_state"].run_status.value, "merged")
            self.assertEqual(result["pool"].market, "CN")
            self.assertTrue(any(record.entity_id == "smic_cn" for record in result["web_research_records"]))

    def test_universe_builder_can_build_cn_bundle_from_file_snapshot(self) -> None:
        universe, pool, companies = build_initial_universe_bundle(
            market="cn",
            universe_id="cn_generated",
            pool_id="cn_generated_pool",
            config_path="config/universe_builder.cn.file.json",
        )
        self.assertEqual(universe.universe_id, "cn_generated")
        self.assertEqual(pool.pool_id, "cn_generated_pool")
        self.assertGreaterEqual(len(universe.eligible_entities), 5)
        self.assertTrue(any(company.current_bucket.value == "core_tracking" for company in companies))

    def test_universe_builder_can_build_cn_bundle_from_hybrid_sources(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeProClient:
            @staticmethod
            def stock_basic(**kwargs) -> FakeFrame:
                return FakeFrame(
                    [
                        {"ts_code": "688981.SH", "symbol": "688981", "name": "SMIC", "industry": "Foundry", "market": "科创板"},
                        {"ts_code": "000063.SZ", "symbol": "000063", "name": "ZTE", "industry": "Network", "market": "主板"},
                        {"ts_code": "300308.SZ", "symbol": "300308", "name": "Zhongji", "industry": "Optical", "market": "创业板"},
                    ]
                )

            @staticmethod
            def daily_basic(**kwargs) -> FakeFrame:
                return FakeFrame(
                    [
                        {"ts_code": "688981.SH", "turnover_rate": 4.6, "volume_ratio": 1.7, "pe_ttm": 42.0, "pb": 1.9, "total_mv": 7200},
                        {"ts_code": "000063.SZ", "turnover_rate": 3.1, "volume_ratio": 1.2, "pe_ttm": 18.0, "pb": 2.2, "total_mv": 2600},
                        {"ts_code": "300308.SZ", "turnover_rate": 5.4, "volume_ratio": 2.1, "pe_ttm": 33.0, "pb": 3.4, "total_mv": 2100},
                    ]
                )

            @staticmethod
            def daily(**kwargs) -> FakeFrame:
                if kwargs.get("ts_code"):
                    return FakeFrame([{"ts_code": kwargs["ts_code"], "trade_date": "20260311"}])
                return FakeFrame(
                    [
                        {"ts_code": "688981.SH", "trade_date": "20260311", "pct_chg": 2.1, "amount": 4200000, "high": 110, "low": 103, "pre_close": 104},
                        {"ts_code": "000063.SZ", "trade_date": "20260311", "pct_chg": 1.2, "amount": 2800000, "high": 38, "low": 36, "pre_close": 36.5},
                        {"ts_code": "300308.SZ", "trade_date": "20260311", "pct_chg": -0.4, "amount": 1900000, "high": 98, "low": 94, "pre_close": 95.5},
                    ]
                )

            @staticmethod
            def moneyflow(**kwargs) -> FakeFrame:
                return FakeFrame([])

            @staticmethod
            def stock_company(**kwargs) -> FakeFrame:
                exchange = kwargs.get("exchange")
                rows = {
                    "SSE": [
                        {
                            "ts_code": "688981.SH",
                            "website": "https://www.smics.com",
                            "introduction": "Foundry and advanced-node manufacturing",
                        }
                    ],
                    "SZSE": [
                        {
                            "ts_code": "000063.SZ",
                            "website": "https://www.zte.com.cn",
                            "main_business": "Telecom equipment and network infrastructure",
                        },
                        {
                            "ts_code": "300308.SZ",
                            "website": "https://www.zj-opto.com",
                            "business_scope": "Optical modules and interconnect products",
                        },
                    ],
                    "BSE": [],
                }
                return FakeFrame(rows[exchange])

        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> FakeProClient:
                return FakeProClient()

        class FakeAkshare:
            @staticmethod
            def stock_zh_a_spot() -> FakeFrame:
                return FakeFrame(
                    [
                        {"代码": "688981", "名称": "SMIC", "成交额": 4300000000, "涨跌幅": 2.3, "量比": 1.9},
                        {"代码": "000063", "名称": "ZTE", "成交额": 2950000000, "涨跌幅": 1.1, "量比": 1.3},
                        {"代码": "300308", "名称": "Zhongji", "成交额": 2050000000, "涨跌幅": -0.2, "量比": 2.4},
                    ]
                )

        import importlib

        real_import_module = importlib.import_module

        def fake_import_module(name: str):
            if name == "tushare":
                return FakeTushare()
            if name == "akshare":
                return FakeAkshare()
            return real_import_module(name)

        with tempfile.TemporaryDirectory() as tmpdir, patch.dict("os.environ", {"TUSHARE_TOKEN": "demo-token"}, clear=False):
            config_path = Path(tmpdir) / "builder_hybrid.json"
            config_path.write_text(
                json.dumps(
                    {
                        "source_kind": "hybrid",
                        "source_priority": ["tushare", "akshare"],
                        "max_entities": 3,
                        "core_size": 1,
                        "secondary_size": 1,
                        "high_beta_size": 1,
                        "function": "stock_zh_a_spot",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch("ai_stock_agent.universe_builder.importlib.import_module", side_effect=fake_import_module):
                universe, pool, companies = build_initial_universe_bundle(
                    market="cn",
                    universe_id="cn_hybrid_generated",
                    pool_id="cn_hybrid_pool",
                    config_path=str(config_path),
                )
        self.assertEqual(universe.universe_rules_version, "universe_rules_hybrid_v3")
        self.assertEqual(len(universe.eligible_entities), 3)
        self.assertTrue(any(entity.ticker == "688981.SH" for entity in universe.eligible_entities))
        self.assertTrue(any(entity.website == "https://www.smics.com" for entity in universe.eligible_entities))
        self.assertTrue(any(entity.business_summary for entity in universe.eligible_entities))
        self.assertTrue(any(company.current_bucket.value == "core_tracking" for company in companies))

    def test_universe_builder_can_enrich_hk_market_cap_and_sector(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeProClient:
            @staticmethod
            def hk_basic() -> FakeFrame:
                return FakeFrame(
                    [
                        {"ts_code": "00700.HK", "name": "腾讯控股", "market": "主板"},
                        {"ts_code": "00981.HK", "name": "中芯国际", "market": "主板"},
                    ]
                )

            @staticmethod
            def hk_daily(**kwargs) -> FakeFrame:
                if kwargs.get("ts_code"):
                    return FakeFrame([{"ts_code": kwargs["ts_code"], "trade_date": "20260311"}])
                return FakeFrame(
                    [
                        {"ts_code": "00700.HK", "trade_date": "20260311", "close": 552.0, "pct_chg": -0.27, "amount": 26800185739.14},
                        {"ts_code": "00981.HK", "trade_date": "20260311", "close": 25.8, "pct_chg": 2.1, "amount": 4370000000.0},
                    ]
                )

        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> FakeProClient:
                return FakeProClient()

        class FakeAkshare:
            @staticmethod
            def stock_hk_spot() -> FakeFrame:
                return FakeFrame(
                    [
                        {"代码": "00700", "中文名称": "腾讯控股", "最新价": 552.0, "涨跌幅": -0.27, "成交额": 26800185739.14},
                        {"代码": "00981", "中文名称": "中芯国际", "最新价": 25.8, "涨跌幅": 2.1, "成交额": 4370000000.0},
                    ]
                )

            @staticmethod
            def stock_hk_financial_indicator_em(symbol: str) -> FakeFrame:
                rows = {
                    "00700": [{"总市值(港元)": 4976623622312.5, "已发行股本(股)": 9106356125, "市盈率": 21.2, "市净率": 3.88}],
                    "00981": [{"总市值(港元)": 164200000000.0, "已发行股本(股)": 6360000000, "市盈率": 17.5, "市净率": 1.26}],
                }
                return FakeFrame(rows[symbol])

            @staticmethod
            def stock_hk_company_profile_em(symbol: str) -> FakeFrame:
                rows = {
                    "00700": [{"所属行业": "软件服务", "英文名称": "Tencent Holdings Limited", "公司介绍": "互联网平台与云服务", "公司网址": "https://www.tencent.com"}],
                    "00981": [{"所属行业": "半导体", "英文名称": "Semiconductor Manufacturing International", "公司介绍": "晶圆代工与先进制程", "公司网址": "https://www.smics.com"}],
                }
                return FakeFrame(rows[symbol])

            @staticmethod
            def stock_hk_security_profile_em(symbol: str) -> FakeFrame:
                return FakeFrame([{"板块": "主板"}])

        import importlib

        real_import_module = importlib.import_module

        def fake_import_module(name: str):
            if name == "tushare":
                return FakeTushare()
            if name == "akshare":
                return FakeAkshare()
            return real_import_module(name)

        with tempfile.TemporaryDirectory() as tmpdir, patch.dict("os.environ", {"TUSHARE_TOKEN": "demo-token"}, clear=False):
            config_path = Path(tmpdir) / "builder_hk_hybrid.json"
            config_path.write_text(
                json.dumps(
                    {
                        "source_kind": "hybrid",
                        "source_priority": ["tushare", "akshare"],
                        "max_entities": 2,
                        "core_size": 1,
                        "secondary_size": 1,
                        "high_beta_size": 0,
                        "candidate_pool_size": 2,
                        "hk_enrichment_max_symbols": 2,
                        "function": "stock_hk_spot",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            with patch("ai_stock_agent.universe_builder.importlib.import_module", side_effect=fake_import_module), patch(
                "ai_stock_agent.hk_enrichment.importlib.import_module",
                side_effect=fake_import_module,
            ):
                universe, pool, companies = build_initial_universe_bundle(
                    market="hk",
                    universe_id="hk_hybrid_generated",
                    pool_id="hk_hybrid_pool",
                    config_path=str(config_path),
                )

        self.assertEqual(universe.universe_rules_version, "universe_rules_hybrid_v3")
        self.assertEqual(pool.pool_id, "hk_hybrid_pool")
        self.assertEqual(len(universe.eligible_entities), 2)
        self.assertTrue(any(entity.market_cap > 1_000_000_000_000 for entity in universe.eligible_entities))
        self.assertTrue(any(entity.sector == "Internet Platform" for entity in universe.eligible_entities))
        self.assertTrue(any(entity.sector == "Semiconductor" for entity in universe.eligible_entities))
        self.assertTrue(any(entity.website == "https://www.tencent.com" for entity in universe.eligible_entities))
        self.assertTrue(any(entity.business_summary for entity in universe.eligible_entities))
        self.assertTrue(any(company.current_bucket.value == "core_tracking" for company in companies))

    def test_hk_workflow_can_run_on_hk_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                universe_id="hk_macro_ai",
                pool_id="hk_macro_ai_pool",
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "news", "ticker": "0700.HK"}],
                web_config_path="config/web_research.free_ah.json",
                data_config_path="config/data_sources.free_ah.json",
            )
            self.assertEqual(result["run_state"].run_status.value, "merged")
            self.assertEqual(result["pool"].market, "HK")
            self.assertTrue(any(record.entity_id == "tencent_hk" for record in result["web_research_records"]))

    def test_file_web_research_config_can_drive_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            web_data_path = Path(tmpdir) / "web_research.json"
            web_config_path = Path(tmpdir) / "web_config.json"
            web_data_path.write_text(
                json.dumps(
                    {
                        "macro_theme": [
                            {
                                "title": "Macro AI infra update",
                                "snippet": "Macro snippet",
                                "url": "https://example.com/macro",
                                "source_name": "example_source",
                                "published_at": "2026-03-10T10:00:00Z",
                                "relevance_score": 0.8,
                                "evidence_type": "macro_news",
                            }
                        ],
                        "entities": {
                            "amd_us": [
                                {
                                    "title": "AMD file research",
                                    "snippet": "AMD snippet",
                                    "url": "https://example.com/amd",
                                    "source_name": "example_source",
                                    "published_at": "2026-03-09T09:00:00Z",
                                    "relevance_score": 0.9,
                                    "evidence_type": "company_news",
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            web_config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_file",
                            "provider_kind": "file",
                            "enabled": True,
                            "file_path": str(web_data_path),
                            "max_results": 3,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                run_mode="event_driven_refresh",
                web_config_path=str(web_config_path),
                incoming_events=[{"event_type": "macro", "ticker": "AMD"}],
            )
            records = [record for record in result["web_research_records"] if record.entity_id == "amd_us"]
            self.assertTrue(records)
            self.assertEqual(records[0].evidence_items[0].title, "AMD file research")
            self.assertGreater(records[0].evidence_items[0].source_score, 0.6)
            self.assertIn(records[0].evidence_items[0].source_tier, {"file_snapshot", "market_data", "official"})

    def test_web_research_records_can_synthesize_trigger_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            web_data_path = Path(tmpdir) / "web_trigger.json"
            web_config_path = Path(tmpdir) / "web_trigger_config.json"
            web_data_path.write_text(
                json.dumps(
                    {
                        "macro_theme": [],
                        "entities": {
                            "nvda_us": [
                                {
                                    "title": "NVIDIA earnings outlook raised",
                                    "snippet": "Management guidance points to stronger accelerator demand.",
                                    "url": "https://example.com/nvda/guidance",
                                    "source_name": "example_source",
                                    "published_at": "2026-03-10T09:00:00Z",
                                    "relevance_score": 0.9,
                                    "evidence_type": "company_news",
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            web_config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_file",
                            "provider_kind": "file",
                            "enabled": True,
                            "file_path": str(web_data_path),
                            "max_results": 3,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                run_mode="periodic_review",
                web_config_path=str(web_config_path),
            )
            trigger_events = result["trigger_events"]
            self.assertTrue(any(event.source_ref == "https://example.com/nvda/guidance" for event in trigger_events))
            self.assertTrue(any(event.event_type.value in {"guidance", "earnings"} for event in trigger_events))
            self.assertTrue(any(event.source_quality_score > 0.6 for event in trigger_events if event.source_ref == "https://example.com/nvda/guidance"))

    def test_official_web_evidence_gets_trigger_priority(self) -> None:
        entity = EntityProfile(
            entity_id="nvda_us",
            ticker="NVDA",
            company_name="NVIDIA",
            sector="Semiconductor",
            market_cap=100,
            liquidity_score=90,
            info_score=90,
            tradability_score=90,
            base_quality=90,
            industry_position_score=90,
            macro_alignment_score=90,
            valuation_score=60,
            catalyst_score=70,
            risk_penalty=25,
            evidence_freshness_days=2,
        )
        records = [
            {
                "record_id": "web_demo",
                "run_id": "run_demo",
                "provider_id": "web_file",
                "macro_theme": "AI infrastructure resilience",
                "query": "NVDA news",
                "entity_id": "nvda_us",
                "evidence_items": [
                    {
                        "title": "NVIDIA blog mentions roadmap",
                        "snippet": "General product roadmap note.",
                        "url": "https://blog.example.com/nvda-roadmap",
                        "source_name": "example_blog",
                        "published_at": "2026-03-10T09:00:00Z",
                        "relevance_score": 0.92,
                        "evidence_type": "company_news",
                    },
                    {
                        "title": "NVIDIA files official guidance update",
                        "snippet": "Investor relations update confirms stronger AI demand.",
                        "url": "https://investor.nvidia.com/news/guidance-update",
                        "source_name": "NVIDIA IR",
                        "published_at": "2026-03-10T08:00:00Z",
                        "relevance_score": 0.83,
                        "source_tier": "official",
                        "source_score": 0.95,
                        "content_hash": "official_guidance_hash",
                        "evidence_type": "company_news",
                    },
                ],
                "summary": "demo",
                "status": "ok",
                "created_at": "2026-03-10T09:00:00Z",
            }
        ]
        from ai_stock_agent.models import WebResearchRecord

        trigger_events = synthesize_trigger_events(
            run_id="run_demo",
            records=[WebResearchRecord.model_validate(item) for item in records],
            entity_map={"nvda_us": entity},
            existing_events=[],
        )
        self.assertEqual(len(trigger_events), 1)
        self.assertEqual(trigger_events[0].source_ref, "https://investor.nvidia.com/news/guidance-update")
        self.assertGreaterEqual(trigger_events[0].source_quality_score, 0.9)

    def test_web_trigger_synthesis_respects_cooldown_across_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            web_data_path = Path(tmpdir) / "web_cooldown.json"
            web_config_path = Path(tmpdir) / "web_cooldown_config.json"
            web_data_path.write_text(
                json.dumps(
                    {
                        "macro_theme": [],
                        "entities": {
                            "nvda_us": [
                                {
                                    "title": "NVIDIA guidance update",
                                    "snippet": "Management sees stronger AI accelerator demand.",
                                    "url": "https://example.com/nvda/guidance",
                                    "source_name": "example_file_source",
                                    "published_at": "2026-03-10T09:00:00Z",
                                    "relevance_score": 0.88,
                                    "evidence_type": "company_news",
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            web_config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_file",
                            "provider_kind": "file",
                            "enabled": True,
                            "file_path": str(web_data_path),
                            "max_results": 3,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            bootstrap_demo_data(db_path)
            first = run_mvp_workflow(
                db_path=db_path,
                run_mode="periodic_review",
                web_config_path=str(web_config_path),
            )
            second = run_mvp_workflow(
                db_path=db_path,
                run_mode="periodic_review",
                web_config_path=str(web_config_path),
            )
            store = SQLiteStateStore(db_path)
            synthesized = [event for event in store.list_trigger_events() if event.source_ref == "https://example.com/nvda/guidance"]
            self.assertEqual(len(synthesized), 1)
            self.assertTrue(any(event.source_ref == "https://example.com/nvda/guidance" for event in first["trigger_events"]))
            self.assertFalse(any(event.source_ref == "https://example.com/nvda/guidance" for event in second["trigger_events"]))

    def test_evidence_gate_blocks_challenger_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                universe_id="cn_macro_ai",
                pool_id="cn_macro_ai_pool",
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "news", "ticker": "688981.SH"}],
                data_config_path="config/data_sources.hybrid_cn.json",
                web_config_path="config/web_research.hybrid_cn.json",
            )
            deltas = result["run_state"].decision_output["deltas"]
            eoptolink_delta = next((delta for delta in deltas if delta["entity_id"] == "eoptolink_cn"), None)
            self.assertIsNotNone(eoptolink_delta)
            self.assertEqual(eoptolink_delta["to_bucket"], "shadow_watch")
            self.assertEqual(eoptolink_delta["route"], "shadow_observation")
            self.assertTrue(any("evidence_gate=" in note for note in result["run_state"].decision_output["notes"]))

    def test_secondary_candidates_can_fall_back_to_shadow_watch(self) -> None:
        validate_bucket_transition(Bucket.SECONDARY_CANDIDATES, Bucket.SHADOW_WATCH)

    def test_official_evidence_boosts_entity_fusion_confidence(self) -> None:
        now_iso = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        entity = EntityProfile(
            entity_id="smic_cn",
            ticker="688981.SH",
            company_name="SMIC",
            sector="Semiconductor",
            market_cap=120_000_000_000,
            liquidity_score=85,
            info_score=88,
            tradability_score=82,
            base_quality=84,
            industry_position_score=83,
            macro_alignment_score=78,
            valuation_score=61,
            catalyst_score=68,
            risk_penalty=28,
            evidence_freshness_days=2,
        )

        class StubManager:
            def get_entity_filing_metrics(self, entity: EntityProfile):
                from ai_stock_agent.models import AdapterPayload, DataSourceHealthRecord, DataSourceManifestItem, SourceType

                return AdapterPayload(
                    data_domain="filing_metrics",
                    values={"data_freshness_hours": 4, "valuation_score": 62.0},
                    source_manifest=[
                        DataSourceManifestItem(
                            source_name="filing_tushare_cn",
                            source_type=SourceType.FILING,
                            as_of=datetime.now(),
                            field_coverage=0.95,
                        )
                    ],
                    health_record=DataSourceHealthRecord(
                        record_id="health_demo",
                        data_domain="filing_metrics",
                        primary_source="filing_tushare_cn",
                        field_completeness=0.95,
                        latency_hours=4,
                        status="healthy",
                        degradation_path="direct",
                    ),
                )

        packet = build_frozen_packet(entity, StubManager())
        from ai_stock_agent.models import WebResearchRecord

        summary = build_entity_evidence_summary(
            run_id="run_demo",
            entity_id="smic_cn",
            packet=packet,
            web_records=[
                WebResearchRecord.model_validate(
                    {
                        "record_id": "web_official",
                        "run_id": "run_demo",
                        "provider_id": "web_file",
                        "macro_theme": "AI infrastructure resilience",
                        "query": "smic",
                        "entity_id": "smic_cn",
                        "evidence_items": [
                            {
                                "title": "SMIC official capacity update",
                                "snippet": "Exchange disclosure confirms new capex cadence.",
                                "url": "https://www.hkex.com.hk/News/SMIC-capacity-update",
                                "source_name": "HKEX",
                                "published_at": now_iso,
                                "relevance_score": 0.82,
                                "source_tier": "official",
                                "source_score": 0.95,
                                "content_hash": "official_hash",
                                "evidence_type": "company_news",
                            }
                        ],
                        "summary": "official only",
                        "status": "ok",
                        "created_at": now_iso,
                    }
                )
            ],
            trigger_events=[],
        )
        self.assertEqual(summary.official_evidence_count, 1)
        self.assertGreaterEqual(summary.web_confidence, 0.8)
        self.assertIn(summary.confidence_label, {"high", "medium"})

    def test_akshare_web_provider_can_collect_cn_and_hk_evidence(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeAkshare:
            @staticmethod
            def news_report_time_baidu(date: str) -> FakeFrame:
                return FakeFrame([{"标题": "Macro flash", "摘要": "Macro body", "发布时间": "2026-03-10 09:00:00"}])

            @staticmethod
            def stock_news_em(symbol: str) -> FakeFrame:
                return FakeFrame([{"新闻标题": "CN company news", "摘要": "CN snippet", "发布时间": "2026-03-10 10:00:00"}])

            @staticmethod
            def stock_zh_a_disclosure_report_cninfo(symbol: str) -> FakeFrame:
                return FakeFrame([{"标题": "CN notice", "具体事项": "Disclosure body", "公告日期": "2026-03-10"}])

            @staticmethod
            def stock_irm_cninfo(symbol: str) -> FakeFrame:
                return FakeFrame([{"问题": "What about AI capex?", "回答": "Still increasing", "发布时间": "2026-03-10"}])

            @staticmethod
            def stock_hk_hot_rank_latest_em(symbol: str) -> FakeFrame:
                return FakeFrame([{"title": "HK hot rank", "snippet": "HK heat", "date": "2026-03-10"}])

            @staticmethod
            def stock_hk_hot_rank_detail_realtime_em(symbol: str) -> FakeFrame:
                return FakeFrame([{"title": "HK detail", "content": "Realtime flow", "date": "2026-03-10"}])

        config_path = Path(tempfile.gettempdir()) / "web_akshare_test.json"
        config_path.write_text(
            json.dumps([{"provider_id": "web_ak", "provider_kind": "akshare", "enabled": True, "max_results": 3}]),
            encoding="utf-8",
        )
        manager = build_web_research_manager(str(config_path))
        entities = [
            EntityProfile(
                entity_id="smic_cn",
                ticker="688981.SH",
                company_name="SMIC",
                sector="Foundry",
                market_cap=100,
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            ),
            EntityProfile(
                entity_id="tencent_hk",
                ticker="0700.HK",
                company_name="Tencent",
                sector="Platform",
                market_cap=100,
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            ),
        ]
        with patch("ai_stock_agent.web_research.importlib.import_module", return_value=FakeAkshare()):
            records = manager.collect(run_id="run_demo", macro_theme="AI infrastructure resilience", trigger_events=[], entities=entities)
        self.assertTrue(any(record.entity_id is None and record.evidence_items for record in records))
        self.assertTrue(any(record.entity_id == "smic_cn" and record.evidence_items for record in records))
        self.assertTrue(any(record.entity_id == "tencent_hk" and record.evidence_items for record in records))

    def test_official_web_provider_can_collect_cn_hk_and_ir_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload_path = Path(tmpdir) / "official_sources.json"
            config_path = Path(tmpdir) / "web_official.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "macro_theme": [
                            {
                                "title": "HKEX official notice",
                                "snippet": "Exchange filing calendar update.",
                                "url": "https://www.hkex.com.hk/notice",
                                "source_name": "HKEX",
                                "published_at": "2026-03-10T08:00:00Z",
                                "relevance_score": 0.8,
                                "evidence_type": "official_market_notice",
                            }
                        ],
                        "entities": {
                            "smic_cn": [
                                {
                                    "title": "中芯国际公告",
                                    "snippet": "CNINFO disclosure",
                                    "url": "https://www.cninfo.com.cn/disclosure/smic",
                                    "source_name": "CNINFO",
                                    "published_at": "2026-03-10T09:00:00Z",
                                    "relevance_score": 0.9,
                                    "evidence_type": "official_disclosure",
                                }
                            ],
                            "0700.HK": [
                                {
                                    "title": "腾讯 HKEX 披露",
                                    "snippet": "HKEXnews filing",
                                    "url": "https://www.hkexnews.hk/tencent",
                                    "source_name": "HKEXnews",
                                    "published_at": "2026-03-10T07:00:00Z",
                                    "relevance_score": 0.88,
                                    "evidence_type": "official_disclosure",
                                }
                            ],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "file_path": str(payload_path),
                            "max_results": 3,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            manager = build_web_research_manager(str(config_path))
            entities = [
                EntityProfile(
                    entity_id="smic_cn",
                    ticker="688981.SH",
                    company_name="SMIC",
                    sector="Foundry",
                    market_cap=100,
                    liquidity_score=80,
                    info_score=80,
                    tradability_score=80,
                    base_quality=80,
                    industry_position_score=80,
                    macro_alignment_score=80,
                    valuation_score=60,
                    catalyst_score=70,
                    risk_penalty=35,
                    evidence_freshness_days=5,
                ),
                EntityProfile(
                    entity_id="tencent_hk",
                    ticker="0700.HK",
                    company_name="Tencent",
                    sector="Platform",
                    market_cap=100,
                    liquidity_score=80,
                    info_score=80,
                    tradability_score=80,
                    base_quality=80,
                    industry_position_score=80,
                    macro_alignment_score=80,
                    valuation_score=60,
                    catalyst_score=70,
                    risk_penalty=35,
                    evidence_freshness_days=5,
                ),
            ]
            records = manager.collect(run_id="run_demo", macro_theme="AI infrastructure resilience", trigger_events=[], entities=entities)
            self.assertTrue(any(record.entity_id is None and record.evidence_items for record in records))
            self.assertTrue(any(record.entity_id == "smic_cn" and record.evidence_items for record in records))
            self.assertTrue(any(record.entity_id == "tencent_hk" and record.evidence_items for record in records))
            official_items = [item for record in records for item in record.evidence_items]
            self.assertTrue(all(item.source_tier == "official" for item in official_items))

    def test_official_web_provider_can_query_cninfo_api(self) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self) -> bytes:
                return self.payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        def fake_urlopen(request, timeout=0):
            self.assertIn("hisAnnouncement/query", request.full_url)
            payload = {
                "announcements": [
                    {
                        "secCode": "688981",
                        "secName": "中芯国际",
                        "announcementTitle": "中芯国际关于产能扩张的公告",
                        "announcementTime": 1773158400000,
                        "adjunctUrl": "finalpage/2026-03-11/1225003790.PDF",
                        "adjunctType": "PDF",
                    },
                    {
                        "secCode": "600519",
                        "secName": "贵州茅台",
                        "announcementTitle": "无关公告",
                        "announcementTime": 1773158400000,
                        "adjunctUrl": "finalpage/2026-03-11/other.PDF",
                        "adjunctType": "PDF",
                    },
                ]
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "web_official_cninfo.json"
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "max_results": 3,
                            "provider_options": {"source_targets": ["cninfo"]},
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            manager = build_web_research_manager(str(config_path))
            entity = EntityProfile(
                entity_id="smic_cn",
                ticker="688981.SH",
                company_name="SMIC",
                sector="Foundry",
                market_cap=100,
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            )
            with patch("ai_stock_agent.official_sources.urlopen", side_effect=fake_urlopen):
                records = manager.collect(
                    run_id="run_demo",
                    macro_theme="AI infrastructure resilience",
                    trigger_events=[],
                    entities=[entity],
                )
            entity_record = next(record for record in records if record.entity_id == "smic_cn")
            self.assertEqual(len(entity_record.evidence_items), 1)
            self.assertEqual(entity_record.evidence_items[0].source_name, "CNINFO")
            self.assertTrue(entity_record.evidence_items[0].url.startswith("https://static.cninfo.com.cn/"))
            self.assertEqual(entity_record.evidence_items[0].source_tier, "official")
            self.assertTrue(any(item.target == "cninfo" and item.status == "ok" for item in entity_record.collection_diagnostics))

    def test_official_web_provider_can_collect_configured_ir_page(self) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self) -> bytes:
                return self.payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        def fake_urlopen(request, timeout=0):
            self.assertEqual(request.full_url, "https://investor.example.com/smic")
            html = """
                <html>
                    <head>
                        <title>SMIC Investor Relations</title>
                        <meta name="description" content="Capacity cadence and capex priorities remain on track." />
                    </head>
                </html>
            """
            return FakeResponse(html.encode("utf-8"))

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "web_official_ir.json"
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "max_results": 2,
                            "provider_options": {
                                "source_targets": ["ir"],
                                "ir_urls": {"smic_cn": "https://investor.example.com/smic"},
                            },
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            manager = build_web_research_manager(str(config_path))
            entity = EntityProfile(
                entity_id="smic_cn",
                ticker="688981.SH",
                company_name="SMIC",
                sector="Foundry",
                market_cap=100,
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            )
            with patch("ai_stock_agent.official_sources.urlopen", side_effect=fake_urlopen):
                records = manager.collect(
                    run_id="run_demo",
                    macro_theme="AI infrastructure resilience",
                    trigger_events=[],
                    entities=[entity],
                )
            entity_record = next(record for record in records if record.entity_id == "smic_cn")
            self.assertEqual(len(entity_record.evidence_items), 1)
            self.assertEqual(entity_record.evidence_items[0].title, "SMIC Investor Relations")
            self.assertEqual(entity_record.evidence_items[0].evidence_type, "official_ir")
            self.assertEqual(entity_record.evidence_items[0].source_tier, "official")
            self.assertTrue(any(item.target == "ir" and item.status == "ok" for item in entity_record.collection_diagnostics))

    def test_official_web_provider_can_discover_ir_from_company_website(self) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self) -> bytes:
                return self.payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        def fake_urlopen(request, timeout=0):
            if request.full_url == "https://example.com":
                html = """
                    <html>
                        <body>
                            <a href="/investor-relations">Investor Relations</a>
                        </body>
                    </html>
                """
                return FakeResponse(html.encode("utf-8"))
            if request.full_url == "https://example.com/investor-relations":
                html = """
                    <html>
                        <head>
                            <title>Example Corp Investor Relations</title>
                            <meta name="description" content="Latest filings and financial presentations." />
                        </head>
                    </html>
                """
                return FakeResponse(html.encode("utf-8"))
            raise AssertionError(f"Unexpected URL {request.full_url}")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "web_official_ir_discovery.json"
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "max_results": 2,
                            "provider_options": {
                                "source_targets": ["ir"],
                                "ir_discovery_max_candidates": 4,
                            },
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            manager = build_web_research_manager(str(config_path))
            entity = EntityProfile(
                entity_id="example_cn",
                ticker="600000.SH",
                company_name="Example Corp",
                sector="Industrial",
                market_cap=100,
                website="https://example.com",
                business_summary="Industrial automation and equipment.",
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            )
            with patch("ai_stock_agent.official_sources.urlopen", side_effect=fake_urlopen):
                records = manager.collect(
                    run_id="run_demo",
                    macro_theme="AI infrastructure resilience",
                    trigger_events=[],
                    entities=[entity],
                )
            entity_record = next(record for record in records if record.entity_id == "example_cn")
            self.assertEqual(len(entity_record.evidence_items), 1)
            self.assertEqual(entity_record.evidence_items[0].title, "Example Corp Investor Relations")
            self.assertEqual(entity_record.evidence_items[0].url, "https://example.com/investor-relations")
            self.assertTrue(any(item.target == "ir_discovery" and item.status == "ok" for item in entity_record.collection_diagnostics))
            self.assertTrue(any(item.target == "ir" and item.status == "ok" for item in entity_record.collection_diagnostics))

    def test_official_web_provider_can_guess_company_website_from_name_rules(self) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def read(self) -> bytes:
                return self.payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        def fake_urlopen(request, timeout=0):
            if request.full_url == "https://www.exampleautomation.com":
                html = """
                    <html>
                        <head>
                            <title>Example Automation</title>
                            <meta name="description" content="Automation systems and industrial control." />
                        </head>
                        <body>
                            <a href="/investors">Investors</a>
                        </body>
                    </html>
                """
                return FakeResponse(html.encode("utf-8"))
            if request.full_url == "https://www.exampleautomation.com/investors":
                html = """
                    <html>
                        <head>
                            <title>Example Automation Investor Relations</title>
                            <meta name="description" content="Earnings decks, governance updates and filings." />
                        </head>
                    </html>
                """
                return FakeResponse(html.encode("utf-8"))
            raise AssertionError(f"Unexpected URL {request.full_url}")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "web_official_website_guess.json"
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "max_results": 2,
                            "provider_options": {
                                "source_targets": ["ir"],
                                "website_tlds": ["com"],
                                "website_rule_max_candidates": 2,
                            },
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            manager = build_web_research_manager(str(config_path))
            entity = EntityProfile(
                entity_id="example_auto_cn",
                ticker="600001.SH",
                company_name="Example Automation",
                english_name="Example Automation Holdings Limited",
                sector="Industrial",
                market_cap=100,
                business_summary="Industrial automation and control systems.",
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            )
            with patch("ai_stock_agent.official_sources.urlopen", side_effect=fake_urlopen):
                records = manager.collect(
                    run_id="run_demo",
                    macro_theme="AI infrastructure resilience",
                    trigger_events=[],
                    entities=[entity],
                )
            entity_record = next(record for record in records if record.entity_id == "example_auto_cn")
            self.assertEqual(len(entity_record.evidence_items), 1)
            self.assertEqual(entity_record.evidence_items[0].url, "https://www.exampleautomation.com/investors")
            self.assertTrue(any(item.target == "website_discovery" and item.status == "ok" for item in entity_record.collection_diagnostics))
            self.assertTrue(any(item.target == "ir_discovery" and item.status == "ok" for item in entity_record.collection_diagnostics))

    def test_official_web_provider_reports_hkex_mapping_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "web_official_hkex.json"
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "max_results": 2,
                            "provider_options": {"source_targets": ["hkex"]},
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            manager = build_web_research_manager(str(config_path))
            entity = EntityProfile(
                entity_id="tencent_hk",
                ticker="0700.HK",
                company_name="Tencent",
                sector="Platform",
                market_cap=100,
                liquidity_score=80,
                info_score=80,
                tradability_score=80,
                base_quality=80,
                industry_position_score=80,
                macro_alignment_score=80,
                valuation_score=60,
                catalyst_score=70,
                risk_penalty=35,
                evidence_freshness_days=5,
            )
            records = manager.collect(
                run_id="run_demo",
                macro_theme="AI infrastructure resilience",
                trigger_events=[],
                entities=[entity],
            )
            entity_record = next(record for record in records if record.entity_id == "tencent_hk")
            self.assertFalse(entity_record.evidence_items)
            self.assertTrue(any(item.target == "hkex" and item.status == "needs_mapping" for item in entity_record.collection_diagnostics))

    def test_workflow_can_use_official_web_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            payload_path = Path(tmpdir) / "official_sources.json"
            config_path = Path(tmpdir) / "web_official.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "macro_theme": [],
                        "entities": {
                            "smic_cn": [
                                {
                                    "title": "中芯国际公告",
                                    "snippet": "CNINFO disclosure",
                                    "url": "https://www.cninfo.com.cn/disclosure/smic",
                                    "source_name": "CNINFO",
                                    "published_at": "2026-03-10T09:00:00Z",
                                    "relevance_score": 0.9,
                                    "evidence_type": "official_disclosure",
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                json.dumps(
                    [
                        {
                            "provider_id": "web_official",
                            "provider_kind": "official",
                            "enabled": True,
                            "file_path": str(payload_path),
                            "max_results": 3,
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            bootstrap_demo_data(db_path)
            result = run_mvp_workflow(
                db_path=db_path,
                universe_id="cn_macro_ai",
                pool_id="cn_macro_ai_pool",
                run_mode="event_driven_refresh",
                incoming_events=[{"event_type": "news", "ticker": "688981.SH"}],
                web_config_path=str(config_path),
            )
            self.assertTrue(any(record.provider_id == "web_official" for record in result["web_research_records"]))
            self.assertTrue(
                any(
                    item.source_tier == "official"
                    for record in result["web_research_records"]
                    for item in record.evidence_items
                )
            )
            self.assertTrue(
                any(
                    diagnostic.target == "file"
                    for record in result["web_research_records"]
                    for diagnostic in record.collection_diagnostics
                )
            )

    def test_akshare_provider_can_derive_market_breadth(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeAkshare:
            @staticmethod
            def stock_zh_a_spot_em() -> FakeFrame:
                return FakeFrame(
                    [
                        {"代码": "600519", "涨跌幅": 1.5, "成交额": 1200000000},
                        {"代码": "000001", "涨跌幅": -0.6, "成交额": 800000000},
                    ]
                )

        config = DataProviderConfig(
            provider_id="ak_cn",
            data_domain="market_breadth",
            mode="primary",
            priority=1,
            provider_kind="akshare",
        )
        with patch("ai_stock_agent.providers.importlib.import_module", return_value=FakeAkshare()):
            payload = make_provider(config).fetch()
        self.assertFalse(payload.degraded)
        self.assertGreater(payload.values["liquidity_breadth"], 0)

    def test_akshare_provider_can_fallback_to_non_em_snapshot_functions(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeAkshare:
            @staticmethod
            def stock_zh_a_spot_em() -> FakeFrame:
                raise Exception("proxy failed")

            @staticmethod
            def stock_zh_a_spot() -> FakeFrame:
                return FakeFrame(
                    [
                        {"代码": "688256", "涨跌幅": 2.4, "成交额": 2100000000, "换手率": 4.8, "市盈率-动态": 85.2},
                        {"代码": "000001", "涨跌幅": -0.3, "成交额": 900000000, "换手率": 1.2, "市盈率-动态": 6.5},
                    ]
                )

        config = DataProviderConfig(
            provider_id="ak_cn",
            data_domain="filing_metrics",
            mode="primary",
            priority=1,
            provider_kind="akshare",
            provider_options={"market": "cn", "function": "stock_zh_a_spot_em"},
        )
        entity = EntityProfile(
            entity_id="cambricon_cn",
            ticker="688256.SH",
            company_name="Cambricon",
            sector="Semiconductor",
            market_cap=120_000_000_000,
            liquidity_score=80,
            info_score=75,
            tradability_score=70,
            base_quality=55,
            industry_position_score=68,
            macro_alignment_score=60,
            valuation_score=35,
            catalyst_score=50,
            risk_penalty=30,
            evidence_freshness_days=5,
        )
        with patch("ai_stock_agent.providers.importlib.import_module", return_value=FakeAkshare()):
            payload = make_provider(config).fetch(entity=entity)
        self.assertFalse(payload.degraded)
        self.assertIsNone(payload.missing_reason)
        self.assertIn("composite_tradability", payload.values)

    def test_tushare_provider_without_token_degrades_cleanly(self) -> None:
        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> object:
                raise AssertionError("pro_api should not be called without token")

        config = DataProviderConfig(
            provider_id="ts_cn",
            data_domain="filing_metrics",
            mode="primary",
            priority=1,
            provider_kind="tushare",
        )
        with patch("ai_stock_agent.providers.importlib.import_module", return_value=FakeTushare()):
            payload = make_provider(config).fetch(
                entity=EntityProfile(
                    entity_id="kweichow",
                    ticker="600519.SH",
                    company_name="Kweichow Moutai",
                    sector="Consumer",
                    market_cap=2_000_000_000_000,
                    liquidity_score=92,
                    info_score=90,
                    tradability_score=88,
                    base_quality=95,
                    industry_position_score=96,
                    macro_alignment_score=60,
                    valuation_score=55,
                    catalyst_score=48,
                    risk_penalty=20,
                    evidence_freshness_days=5,
                )
            )
        self.assertTrue(payload.degraded)
        self.assertEqual(payload.missing_reason, "missing_tushare_token")

    def test_tushare_provider_can_fallback_to_daily_quotes_for_cn(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeProClient:
            @staticmethod
            def daily_basic(ts_code: str, limit: int = 1) -> FakeFrame:
                raise Exception("permission denied")

            @staticmethod
            def daily(ts_code: str, limit: int = 1) -> FakeFrame:
                return FakeFrame(
                    [
                        {
                            "ts_code": ts_code,
                            "trade_date": "20260311",
                            "pct_chg": 3.6,
                            "amount": 4200000000,
                            "vol": 9800000,
                        }
                    ]
                )

        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> FakeProClient:
                return FakeProClient()

        entity = EntityProfile(
            entity_id="smic_cn",
            ticker="688981.SH",
            company_name="SMIC",
            sector="Foundry",
            market_cap=100,
            liquidity_score=80,
            info_score=80,
            tradability_score=80,
            base_quality=80,
            industry_position_score=80,
            macro_alignment_score=80,
            valuation_score=60,
            catalyst_score=70,
            risk_penalty=35,
            evidence_freshness_days=5,
        )
        config = DataProviderConfig(
            provider_id="ts_cn",
            data_domain="filing_metrics",
            mode="primary",
            priority=1,
            provider_kind="tushare",
            provider_options={"token": "demo-token"},
        )
        with patch("ai_stock_agent.providers.importlib.import_module", return_value=FakeTushare()):
            payload = make_provider(config).fetch(entity=entity)
        self.assertFalse(payload.degraded)
        self.assertEqual(payload.health_record.status, "partial")
        self.assertIn("catalyst_score", payload.values)
        self.assertIn("composite_tradability", payload.values)

    def test_tushare_market_breadth_can_fallback_to_daily_proxy(self) -> None:
        class FakeFrame:
            def __init__(self, records: list[dict]) -> None:
                self._records = records

            def to_dict(self, orient: str = "records") -> list[dict]:
                return self._records

        class FakeProClient:
            @staticmethod
            def daily_info(**kwargs) -> FakeFrame:
                return FakeFrame([])

            @staticmethod
            def daily(**kwargs) -> FakeFrame:
                if kwargs.get("ts_code"):
                    return FakeFrame([{"ts_code": "688981.SH", "trade_date": "20260311", "pct_chg": 1.5, "amount": 120000}])
                return FakeFrame(
                    [
                        {"ts_code": "688981.SH", "trade_date": "20260311", "pct_chg": 1.5, "amount": 120000},
                        {"ts_code": "000001.SZ", "trade_date": "20260311", "pct_chg": -0.4, "amount": 90000},
                    ]
                )

        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> FakeProClient:
                return FakeProClient()

        config = DataProviderConfig(
            provider_id="ts_cn_breadth",
            data_domain="market_breadth",
            mode="primary",
            priority=1,
            provider_kind="tushare",
            auth_env_var="TUSHARE_TOKEN",
            provider_options={"market": "cn", "breadth_api": "daily_info"},
        )
        with patch("ai_stock_agent.providers.importlib.import_module", return_value=FakeTushare()), patch.dict(
            os.environ, {"TUSHARE_TOKEN": "demo-token"}, clear=False
        ):
            payload = make_provider(config).fetch(trigger_events=[])
        self.assertFalse(payload.degraded)
        self.assertEqual(payload.health_record.degradation_path, "tushare_market_breadth:daily_proxy")
        self.assertGreater(payload.values["liquidity_breadth"], 0)

    def test_alltick_provider_can_parse_quote_payload(self) -> None:
        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "data": [
                            {
                                "turnover": 350000000,
                                "changeRatio": 2.3,
                                "date": "20260310",
                            }
                        ]
                    }
                ).encode("utf-8")

        entity = EntityProfile(
            entity_id="tencent_hk",
            ticker="0700.HK",
            company_name="Tencent",
            sector="Internet",
            market_cap=3_000_000_000_000,
            liquidity_score=91,
            info_score=88,
            tradability_score=86,
            base_quality=89,
            industry_position_score=88,
            macro_alignment_score=66,
            valuation_score=58,
            catalyst_score=62,
            risk_penalty=28,
            evidence_freshness_days=3,
        )
        config = DataProviderConfig(
            provider_id="alltick_hk",
            data_domain="filing_metrics",
            mode="primary",
            priority=1,
            provider_kind="alltick",
            base_url="https://quote.alltick.io",
            endpoint="/quote",
            provider_options={"token": "demo-token", "response_path": ["data"], "symbol_template": "{ticker}.HK"},
        )
        with patch("ai_stock_agent.providers.urlopen", return_value=FakeResponse()):
            payload = make_provider(config).fetch(entity=entity)
        self.assertFalse(payload.degraded)
        self.assertIn("composite_tradability", payload.values)
        expected_hours = max(0, (date.today() - datetime.strptime("20260310", "%Y%m%d").date()).days * 24)
        self.assertEqual(payload.values["data_freshness_hours"], expected_hours)

    def test_data_source_manager_uses_later_fallback_after_failed_fallback(self) -> None:
        manager = DataSourceManager(
            [
                DataProviderConfig(
                    provider_id="broken_primary",
                    data_domain="market_breadth",
                    mode="primary",
                    priority=1,
                    provider_kind="http",
                    enabled=True,
                ),
                DataProviderConfig(
                    provider_id="broken_fallback",
                    data_domain="market_breadth",
                    mode="fallback",
                    priority=2,
                    provider_kind="akshare",
                    enabled=True,
                ),
                DataProviderConfig(
                    provider_id="mock_fallback",
                    data_domain="market_breadth",
                    mode="fallback",
                    priority=3,
                    provider_kind="mock",
                    enabled=True,
                    field_completeness=0.95,
                ),
            ]
        )
        payload = manager.get_market_breadth([])
        self.assertFalse(payload.degraded)
        self.assertIn("liquidity_breadth", payload.values)
        self.assertIn("advance_ratio", payload.values)
        self.assertIn("breadth_thrust", payload.values)

    def test_knowledge_ingest_can_segment_and_classify_document(self) -> None:
        payload = {
            "title": "China macro and consensus memo",
            "source_name": "desk_note",
            "source_type": "curated_note",
            "region": "cn",
            "published_at": "2026-03-14T09:00:00Z",
            "content": (
                "两会后市场对于稳增长政策的预期明显抬升，财政发力和信用扩张被认为会在二季度逐步体现。"
                "\n\n"
                "当前主流判断认为A股科技主线仍然最强，但拥挤度已经显著上升，尤其是算力和机器人题材。"
                "\n\n"
                "我的投资原则是，当一致预期过热而估值缺乏安全边际时，不应因为情绪高涨而扩大仓位。"
            ),
        }
        document, slices, collections = ingest_knowledge_payload(payload)
        self.assertEqual(document.region, "cn")
        self.assertGreaterEqual(len(slices), 3)
        self.assertTrue(any(item.layer == "macro" for item in slices))
        self.assertTrue(any(item.layer == "consensus" for item in slices))
        self.assertTrue(any(item.layer == "principle" for item in slices))
        self.assertTrue(any(item.crowding_score > 0 for item in slices if item.layer == "consensus"))
        self.assertTrue(collections)

    def test_knowledge_policy_defaults_match_cn_weighting(self) -> None:
        policy = default_knowledge_policy()
        self.assertEqual(policy.macro_weight, 0.62)
        self.assertEqual(policy.consensus_weight, 0.23)
        self.assertEqual(policy.principle_weight, 0.15)
        self.assertEqual(policy.consensus_mode, "crowding_risk_only")
        self.assertEqual(policy.principle_mode, "light_gate")

    def test_refresh_knowledge_marks_stale_and_retired_slices(self) -> None:
        _, slices, _ = ingest_knowledge_payload(
            {
                "title": "Lifecycle memo",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2025-01-01T09:00:00Z",
                "content": "China policy remains important. Current consensus is crowded. My principle is valuation discipline.",
            }
        )
        refreshed = refresh_slice_statuses(slices)
        self.assertTrue(all(item.status == "retired" for item in refreshed))

    def test_load_knowledge_document_payloads_supports_documents_wrapper(self) -> None:
        payloads = load_knowledge_document_payloads(
            json.dumps(
                {
                    "documents": [
                        {
                            "title": "Macro note",
                            "source_name": "desk_note",
                            "source_type": "curated_note",
                            "region": "cn",
                            "content": "China policy support is improving.",
                        },
                        {
                            "title": "Consensus note",
                            "source_name": "desk_note",
                            "source_type": "curated_note",
                            "region": "cn",
                            "content": "Current market consensus is crowded around AI.",
                        },
                    ]
                }
            )
        )
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]["title"], "Macro note")

    def test_prepare_notebooklm_documents_produces_layered_batch(self) -> None:
        documents = prepare_notebooklm_documents(
            text=(
                "China policy support and credit stabilization are improving the macro backdrop. "
                "Current market consensus is crowded around AI leaders and much of the optimism is priced in. "
                "My investment principle is to wait for a better margin of safety."
            ),
            region="cn",
            published_at="2026-03-14T09:00:00Z",
        )
        self.assertGreaterEqual(len(documents), 3)
        self.assertTrue(any(item["layer_hint"] == "macro" for item in documents))
        self.assertTrue(any(item["layer_hint"] == "consensus" for item in documents))
        self.assertTrue(any(item["layer_hint"] == "principle" for item in documents))

    def test_dashboard_helpers_can_prepare_and_ingest_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            prepared = prepare_notebooklm_batch(
                "China policy support is improving. Current market consensus is crowded. My investment principle is valuation discipline.",
                region="cn",
                published_at="2026-03-14T09:00:00Z",
            )
            records = ingest_knowledge_batch(db_path, prepared["documents"])
            self.assertTrue(records)
            snapshot = build_dashboard_snapshot(db_path, market="cn", status="all")
            self.assertTrue(snapshot["universes"])
            self.assertTrue(snapshot["knowledge_documents"])

    def test_dashboard_snapshot_exposes_pool_companies_company_detail_and_topic_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            ingest_knowledge_batch(
                db_path,
                [
                    {
                        "title": "China AI view",
                        "topic_key": "china_ai_view",
                        "source_name": "desk_note",
                        "source_type": "curated_note",
                        "region": "cn",
                        "published_at": "2026-03-10T09:00:00Z",
                        "content": "China policy support is improving the macro backdrop.",
                    },
                    {
                        "title": "China AI view",
                        "topic_key": "china_ai_view",
                        "source_name": "desk_note",
                        "source_type": "curated_note",
                        "region": "cn",
                        "published_at": "2026-03-14T09:00:00Z",
                        "content": "China policy support is improving. Current market consensus is crowded around AI.",
                    }
                ],
            )
            run_mvp_workflow(
                db_path=db_path,
                run_mode="event_driven_refresh",
                universe_id="cn_macro_ai",
                pool_id="cn_macro_ai_pool",
                macro_theme="China AI policy resilience",
                incoming_events=[{"event_type": "news", "ticker": "688981.SH"}],
            )
            snapshot = build_dashboard_snapshot(
                db_path,
                market="cn",
                pool_id="cn_macro_ai_pool",
                status="all",
            )
            self.assertEqual(snapshot["selected_pool_id"], "cn_macro_ai_pool")
            self.assertTrue(snapshot["pool_companies"])
            self.assertIsNotNone(snapshot["company_detail"])
            self.assertTrue(snapshot["topic_tree"])
            self.assertIn("evidence_summaries", snapshot)
            self.assertIn("official_status", snapshot)
            focused_snapshot = build_dashboard_snapshot(
                db_path,
                market="cn",
                pool_id="cn_macro_ai_pool",
                topic_key="china_ai_view",
                status="all",
            )
            self.assertIsNotNone(focused_snapshot["topic_diff"])
            self.assertEqual(focused_snapshot["topic_diff"]["stats"]["from_version"], 1)
            self.assertEqual(focused_snapshot["topic_diff"]["stats"]["to_version"], 2)
            self.assertGreaterEqual(
                focused_snapshot["topic_diff"]["stats"]["added"] + focused_snapshot["topic_diff"]["stats"]["removed"],
                1,
            )

    def test_dashboard_actions_can_bootstrap_refresh_override_and_build_universe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_counts = bootstrap_demo_action(db_path)
            self.assertGreaterEqual(bootstrap_counts["universes"], 1)
            ingest_knowledge_batch(
                db_path,
                [
                    {
                        "title": "Lifecycle memo",
                        "source_name": "desk_note",
                        "source_type": "curated_note",
                        "region": "cn",
                        "published_at": "2025-01-01T09:00:00Z",
                        "content": "China policy remains important. Current market consensus is crowded. My principle is valuation discipline.",
                    }
                ],
            )
            refresh_counts = refresh_knowledge_state(db_path)
            self.assertGreaterEqual(refresh_counts["total"], 2)
            override = execute_manual_override_action(
                db_path,
                {
                    "entity_id": "smic_cn",
                    "pool_id": "cn_macro_ai_pool",
                    "field": "thesis_status",
                    "value": "fragile",
                    "reason": "gui test",
                    "operator": "gui_operator",
                },
            )
            self.assertEqual(override["override_field"], "thesis_status")
            build_result = build_universe_action(
                db_path,
                {
                    "market": "cn",
                    "universe_id": "cn_gui_generated",
                    "pool_id": "cn_gui_generated_pool",
                    "builder_config": "config/universe_builder.cn.file.json",
                },
            )
            self.assertEqual(build_result["universe"]["universe_id"], "cn_gui_generated")

    def test_query_knowledge_slices_can_filter_by_layer_and_topic(self) -> None:
        _, slices, _ = ingest_knowledge_payload(
            {
                "title": "Layered memo",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2026-03-14T09:00:00Z",
                "content": (
                    "China policy support and credit stabilization are improving the macro backdrop. "
                    "Current market consensus is crowded around AI leaders. "
                    "My investment principle is to wait for a better margin of safety."
                ),
            }
        )
        macro_only = query_knowledge_slices(
            slices=slices,
            market="cn",
            macro_theme="China AI policy resilience",
            layer="macro",
            status="active",
        )
        consensus_only = query_knowledge_slices(
            slices=slices,
            market="cn",
            macro_theme="China AI policy resilience",
            layer="consensus",
            topic_tags=["crowding"],
            status="active",
        )
        self.assertTrue(macro_only)
        self.assertTrue(all(item.layer == "macro" for item in macro_only))
        self.assertEqual(len(consensus_only), 1)
        self.assertEqual(consensus_only[0].layer, "consensus")

        topic_filtered = query_knowledge_slices(
            slices=slices,
            market="cn",
            topic_key=slices[0].topic_key,
            status="active",
        )
        self.assertTrue(topic_filtered)
        self.assertTrue(all(item.topic_key == slices[0].topic_key for item in topic_filtered))

    def test_resolve_document_version_supersedes_previous_topic_version(self) -> None:
        from ai_stock_agent.knowledge_base import resolve_document_version

        first_document, first_slices, first_collections = ingest_knowledge_payload(
            {
                "title": "China AI view",
                "topic_key": "china_ai_view",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2026-03-10T09:00:00Z",
                "content": "China policy support is improving the macro backdrop.",
            }
        )
        second_document, second_slices, second_collections = ingest_knowledge_payload(
            {
                "title": "China AI view",
                "topic_key": "china_ai_view",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2026-03-14T09:00:00Z",
                "content": "China policy support remains, but market consensus is now crowded around AI.",
            }
        )
        resolved_document, resolved_slices, resolved_collections, superseded_documents, retired_slices, retired_collections = resolve_document_version(
            document=second_document,
            slices=second_slices,
            collections=second_collections,
            existing_documents=[first_document],
            existing_slices=first_slices,
            existing_collections=first_collections,
        )
        self.assertEqual(resolved_document.version, 2)
        self.assertEqual(resolved_document.supersedes_document_id, first_document.document_id)
        self.assertEqual(len(superseded_documents), 1)
        self.assertEqual(superseded_documents[0].status, "superseded")
        self.assertEqual(superseded_documents[0].superseded_by_document_id, resolved_document.document_id)
        self.assertTrue(all(item.version == 2 for item in resolved_slices))
        self.assertTrue(all(item.status == "retired" for item in retired_slices))
        self.assertTrue(all(item.topic_key == "china_ai_view" for item in resolved_collections))
        self.assertTrue(all(item.status == "retired" for item in retired_collections))

    def test_build_topic_version_diff_reports_document_and_slice_changes(self) -> None:
        first_document, first_slices, _ = ingest_knowledge_payload(
            {
                "title": "China AI view",
                "topic_key": "china_ai_view",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2026-03-10T09:00:00Z",
                "content": "China policy support is improving the macro backdrop.",
            }
        )
        second_document, second_slices, _ = ingest_knowledge_payload(
            {
                "title": "China AI view",
                "topic_key": "china_ai_view",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2026-03-14T09:00:00Z",
                "content": "China policy support remains, but market consensus is now crowded around AI.",
            }
        )
        second_document = second_document.model_copy(update={"version": 2, "supersedes_document_id": first_document.document_id})
        second_slices = [
            item.model_copy(
                update={
                    "document_id": second_document.document_id,
                    "topic_key": second_document.topic_key,
                    "version": 2,
                }
            )
            for item in second_slices
        ]
        diff = build_topic_version_diff(
            documents=[first_document, second_document],
            slices=[*first_slices, *second_slices],
            topic_key="china_ai_view",
            region="cn",
        )
        self.assertIsNotNone(diff)
        self.assertEqual(diff["stats"]["from_version"], 1)
        self.assertEqual(diff["stats"]["to_version"], 2)
        self.assertGreaterEqual(diff["stats"]["document_fields_changed"], 1)
        self.assertGreaterEqual(diff["stats"]["added"] + diff["stats"]["removed"], 1)
        self.assertIn("v1 -> v2", diff["summary"])
        self.assertTrue(diff["takeaways"])

    def test_dashboard_snapshot_can_compare_explicit_topic_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            ingest_knowledge_batch(
                db_path,
                [
                    {
                        "title": "China AI view",
                        "topic_key": "china_ai_view",
                        "source_name": "desk_note",
                        "source_type": "curated_note",
                        "region": "cn",
                        "published_at": "2026-03-10T09:00:00Z",
                        "content": "China policy support is improving the macro backdrop.",
                    },
                    {
                        "title": "China AI view",
                        "topic_key": "china_ai_view",
                        "source_name": "desk_note",
                        "source_type": "curated_note",
                        "region": "cn",
                        "published_at": "2026-03-12T09:00:00Z",
                        "content": "China policy support remains, and export demand is stabilizing.",
                    },
                    {
                        "title": "China AI view",
                        "topic_key": "china_ai_view",
                        "source_name": "desk_note",
                        "source_type": "curated_note",
                        "region": "cn",
                        "published_at": "2026-03-14T09:00:00Z",
                        "content": "China policy support remains, but market consensus is now crowded around AI.",
                    },
                ],
            )
            snapshot = build_dashboard_snapshot(
                db_path,
                market="cn",
                topic_key="china_ai_view",
                from_version=1,
                to_version=3,
                status="all",
            )
            self.assertIsNotNone(snapshot["topic_diff"])
            self.assertEqual(snapshot["topic_diff"]["stats"]["from_version"], 1)
            self.assertEqual(snapshot["topic_diff"]["stats"]["to_version"], 3)
            self.assertEqual(len(snapshot["topic_diff"]["available_versions"]), 3)

    def test_knowledge_context_builds_layer_signals(self) -> None:
        _, slices, _ = ingest_knowledge_payload(
            {
                "title": "China market memo",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "published_at": "2026-03-14T09:00:00Z",
                "content": (
                    "China policy support and credit stabilization are improving the macro backdrop. "
                    "Current market consensus is crowded around AI leaders and much of the optimism is already priced in. "
                    "My investment principle is to wait for a better margin of safety when valuation is stretched."
                ),
            }
        )
        context = build_knowledge_context(
            run_id="run_demo",
            macro_theme="China AI policy resilience",
            market="cn",
            slices=slices,
            policy=default_knowledge_policy(),
        )
        self.assertGreater(context.macro_signal.score, 0)
        self.assertLess(context.consensus_signal.score, 0)
        self.assertTrue(
            {"high_crowding", "crowded_consensus"}.intersection(context.consensus_signal.risk_flags)
        )
        self.assertLess(context.principle_signal.score, 0)
        self.assertIn("valuation_discipline", context.principle_signal.risk_flags)

    def test_scorecard_applies_knowledge_crowding_penalty(self) -> None:
        from ai_stock_agent.models import WeightCalibrationPolicy

        entity = EntityProfile(
            entity_id="smic_cn",
            ticker="688981.SH",
            company_name="SMIC",
            sector="Semiconductor",
            market_cap=520_000_000_000,
            liquidity_score=82,
            info_score=84,
            tradability_score=80,
            base_quality=78,
            industry_position_score=74,
            macro_alignment_score=73,
            valuation_score=58,
            catalyst_score=76,
            risk_penalty=38,
            evidence_freshness_days=6,
            active_factor_exposures=["ai_compute", "foundry"],
        )

        class StubManager:
            @staticmethod
            def get_entity_filing_metrics(entity: EntityProfile):
                from ai_stock_agent.models import AdapterPayload, DataSourceHealthRecord, DataSourceManifestItem, SourceType

                return AdapterPayload(
                    data_domain="filing_metrics",
                    values={"data_freshness_hours": 4},
                    source_manifest=[
                        DataSourceManifestItem(
                            source_name="stub_filing",
                            source_type=SourceType.FILING,
                            as_of=datetime.now(),
                            field_coverage=0.95,
                        )
                    ],
                    health_record=DataSourceHealthRecord(
                        record_id="health_stub",
                        data_domain="filing_metrics",
                        primary_source="stub_filing",
                        field_completeness=0.95,
                        latency_hours=4,
                        status="healthy",
                        degradation_path="direct",
                    ),
                )

        packet = build_frozen_packet(entity, StubManager())
        _, slices, _ = ingest_knowledge_payload(
            {
                "title": "Consensus note",
                "source_name": "desk_note",
                "source_type": "curated_note",
                "region": "cn",
                "content": (
                    "Current market consensus is crowded around AI leaders and the trade looks priced in. "
                    "My investment principle is to avoid adding size when valuation has no margin of safety."
                ),
            }
        )
        knowledge_context = build_knowledge_context(
            run_id="run_demo",
            macro_theme="China AI policy resilience",
            market="cn",
            slices=slices,
            policy=default_knowledge_policy(),
        )
        baseline = compute_scorecard(
            entity,
            packet,
            WeightCalibrationPolicy(),
            True,
        )
        adjusted = compute_scorecard(
            entity,
            packet,
            WeightCalibrationPolicy(),
            True,
            knowledge_context=knowledge_context,
        )
        self.assertLess(adjusted.trajectory_score, baseline.trajectory_score)
        self.assertLess(adjusted.retention_priority_score, baseline.retention_priority_score)
        self.assertTrue(any("knowledge_crowding_penalty=" in note for note in adjusted.notes))

    def test_knowledge_records_persist_in_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            store = SQLiteStateStore(db_path)
            document, slices, collections = ingest_knowledge_payload(
                {
                    "title": "A-share crowding note",
                    "source_name": "desk_note",
                    "source_type": "curated_note",
                    "region": "cn",
                    "content": "当前主流判断认为AI主线最强，但交易已经明显拥挤。",
                }
            )
            store.save_knowledge_document(document)
            for item in slices:
                store.save_knowledge_slice(item)
            for item in collections:
                store.save_knowledge_collection(item)
            store.save_knowledge_policy(default_knowledge_policy())
            self.assertEqual(len(store.list_knowledge_documents()), 1)
            self.assertGreaterEqual(len(store.list_knowledge_slices()), 1)
            self.assertGreaterEqual(len(store.list_knowledge_collections()), 1)
            self.assertEqual(len(store.list_knowledge_policies()), 1)

    def test_workflow_persists_knowledge_context_overlay(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "agent.db")
            bootstrap_demo_data(db_path)
            store = SQLiteStateStore(db_path)
            document, slices, collections = ingest_knowledge_payload(
                {
                    "title": "China AI macro overlay",
                    "source_name": "desk_note",
                    "source_type": "curated_note",
                    "region": "cn",
                    "published_at": "2026-03-14T09:00:00Z",
                    "content": (
                        "China policy support is improving the macro backdrop for domestic technology. "
                        "Current market consensus is crowded around AI and much of the optimism is priced in. "
                        "My investment principle is to wait for a better margin of safety."
                    ),
                }
            )
            store.save_knowledge_document(document)
            for item in slices:
                store.save_knowledge_slice(item)
            for item in collections:
                store.save_knowledge_collection(item)
            store.save_knowledge_policy(default_knowledge_policy())
            result = run_mvp_workflow(
                db_path=db_path,
                run_mode="event_driven_refresh",
                universe_id="cn_macro_ai",
                pool_id="cn_macro_ai_pool",
                macro_theme="China AI policy resilience",
                incoming_events=[{"event_type": "macro", "ticker": "688981.SH", "source_ref": "test"}],
            )
            self.assertIn("knowledge_context", result)
            self.assertTrue(store.list_knowledge_contexts())
            self.assertTrue(any(note.startswith("knowledge_overlay=") for note in result["merge_notes"]))

    def test_tushare_probe_reports_partial_ready_profile(self) -> None:
        class FakeFrame:
            def __init__(self, rows: int = 1, columns: list[str] | None = None) -> None:
                self._rows = rows
                self.columns = columns or ["ts_code", "close"]

            def __len__(self) -> int:
                return self._rows

        class FakeProClient:
            @staticmethod
            def stock_basic(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "symbol", "name"])

            @staticmethod
            def daily(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "trade_date", "close"])

            @staticmethod
            def daily_basic(**kwargs) -> FakeFrame:
                raise Exception("permission denied")

            @staticmethod
            def daily_info(**kwargs) -> FakeFrame:
                raise Exception("permission denied")

            @staticmethod
            def moneyflow(**kwargs) -> FakeFrame:
                raise Exception("permission denied")

            @staticmethod
            def hk_basic(**kwargs) -> FakeFrame:
                raise Exception("permission denied")

            @staticmethod
            def hk_daily(**kwargs) -> FakeFrame:
                raise Exception("permission denied")

        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> FakeProClient:
                return FakeProClient()

        with patch.dict("sys.modules", {"tushare": FakeTushare()}):
            result = probe_tushare_access("demo-token")
        self.assertEqual(result["recommended_profile"], "cn_partial_ready")
        self.assertTrue(any(item["name"] == "stock_basic_cn" and item["status"] == "ok" for item in result["results"]))

    def test_tushare_probe_reports_cross_market_research_ready_profile(self) -> None:
        class FakeFrame:
            def __init__(self, rows: int = 1, columns: list[str] | None = None) -> None:
                self._rows = rows
                self.columns = columns or ["ts_code", "close"]

            def __len__(self) -> int:
                return self._rows

        class FakeProClient:
            @staticmethod
            def stock_basic(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "symbol", "name"])

            @staticmethod
            def daily(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "trade_date", "close"])

            @staticmethod
            def daily_basic(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "trade_date", "pe_ttm"])

            @staticmethod
            def daily_info(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["trade_date", "total_mv"])

            @staticmethod
            def moneyflow(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "buy_sm_vol"])

            @staticmethod
            def hk_basic(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "name", "list_date"])

            @staticmethod
            def hk_daily(**kwargs) -> FakeFrame:
                return FakeFrame(columns=["ts_code", "trade_date", "close"])

        class FakeTushare:
            @staticmethod
            def pro_api(token: str) -> FakeProClient:
                return FakeProClient()

        with patch.dict("sys.modules", {"tushare": FakeTushare()}):
            result = probe_tushare_access("demo-token")
        self.assertEqual(result["recommended_profile"], "cross_market_research_ready")
        self.assertTrue(any(item["name"] == "hk_basic" and item["status"] == "ok" for item in result["results"]))


if __name__ == "__main__":
    unittest.main()
