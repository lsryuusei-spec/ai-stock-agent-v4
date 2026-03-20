from __future__ import annotations

from typing import Any, Callable

from .runtime_env import read_env_value


ProbeCall = Callable[[Any], Any]


def _frame_summary(frame: Any) -> tuple[int, list[str]]:
    if frame is None:
        return 0, []
    rows = 0
    columns: list[str] = []
    if hasattr(frame, "__len__"):
        try:
            rows = len(frame)
        except TypeError:
            rows = 0
    if hasattr(frame, "columns"):
        try:
            columns = list(frame.columns)
        except TypeError:
            columns = []
    return rows, columns[:8]


def _probe_step(client: Any, name: str, fn: ProbeCall) -> dict[str, Any]:
    try:
        frame = fn(client)
        rows, columns = _frame_summary(frame)
        return {
            "name": name,
            "status": "ok",
            "rows": rows,
            "columns": columns,
            "message": None,
        }
    except Exception as exc:  # pragma: no cover - depends on live vendor responses
        return {
            "name": name,
            "status": "blocked",
            "rows": 0,
            "columns": [],
            "message": str(exc),
        }


def probe_tushare_access(token: str | None = None) -> dict[str, Any]:
    resolved_token = token or read_env_value("TUSHARE_TOKEN")
    if not resolved_token:
        return {
            "status": "missing_token",
            "token_source": "none",
            "results": [],
            "recommended_profile": "fallback_only",
        }
    try:
        import tushare as ts
    except ImportError:
        return {
            "status": "sdk_missing",
            "token_source": "env_or_registry",
            "results": [],
            "recommended_profile": "fallback_only",
        }

    client = ts.pro_api(resolved_token)
    checks: list[tuple[str, ProbeCall]] = [
        ("stock_basic_cn", lambda pro: pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,industry,market")),
        ("daily_basic_cn", lambda pro: pro.daily_basic(ts_code="688981.SH", limit=1)),
        ("daily_cn", lambda pro: pro.daily(ts_code="688981.SH", limit=1)),
        ("daily_info_sse", lambda pro: pro.daily_info(exchange="SSE")),
        ("moneyflow_cn", lambda pro: pro.moneyflow(ts_code="688981.SH", limit=1)),
        ("hk_basic", lambda pro: pro.hk_basic()),
        ("hk_daily", lambda pro: pro.hk_daily(ts_code="00700.HK", limit=1)),
    ]
    results = [_probe_step(client, name, fn) for name, fn in checks]
    accessible = {item["name"] for item in results if item["status"] == "ok"}

    cn_research_core = {"stock_basic_cn", "daily_basic_cn", "daily_info_sse", "moneyflow_cn"}
    hk_research_core = {"hk_basic", "hk_daily"}

    if cn_research_core.issubset(accessible) and hk_research_core.issubset(accessible):
        profile = "cross_market_research_ready"
    elif cn_research_core.issubset(accessible):
        profile = "cn_research_ready"
    elif {"daily_basic_cn", "daily_info_sse"}.issubset(accessible):
        profile = "cn_primary_ready"
    elif {"stock_basic_cn", "daily_cn"}.issubset(accessible):
        profile = "cn_partial_ready"
    elif "hk_daily" in accessible:
        profile = "hk_quote_ready"
    else:
        profile = "fallback_only"

    return {
        "status": "ok",
        "token_source": "env_or_registry",
        "results": results,
        "recommended_profile": profile,
    }
