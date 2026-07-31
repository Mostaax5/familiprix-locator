import os
import threading
import time
from collections import deque

from memory_guard import memory_snapshot


_STARTED_AT = time.time()
_LOCK = threading.Lock()
_MAX_ROUTE_SERIES = 48
_REQUEST_SAMPLES = {}
_AI_SAMPLES = deque(maxlen=240)
_LAST_WARNING_LOG_AT = 0.0


def _percentile(values, percentile):
    if not values:
        return 0
    ordered = sorted(float(value) for value in values)
    index = min(
        len(ordered) - 1,
        max(0, int(round((len(ordered) - 1) * percentile))),
    )
    return int(round(ordered[index]))


def record_request(method, route, status_code, elapsed_ms):
    key = f"{str(method or 'GET').upper()} {str(route or 'unknown')}"[:160]
    if key in {
        "GET /healthz", "HEAD /healthz", "GET /readyz",
        "GET /api/ops/status",
    }:
        return
    with _LOCK:
        series = _REQUEST_SAMPLES.get(key)
        if series is None:
            # Reserve the final bucket for routes outside the bounded top set.
            if len(_REQUEST_SAMPLES) >= _MAX_ROUTE_SERIES - 1:
                key = "OTHER"
                series = _REQUEST_SAMPLES.get(key)
            if series is None:
                series = {
                    "latencies": deque(maxlen=240),
                    "requests": 0, "errors": 0, "last_status": 0,
                    "last_seen_at": 0.0,
                }
                _REQUEST_SAMPLES[key] = series
        series["latencies"].append(max(0.0, float(elapsed_ms or 0)))
        series["requests"] += 1
        series["errors"] += int(int(status_code or 0) >= 500)
        series["last_status"] = int(status_code or 0)
        series["last_seen_at"] = time.time()


def record_ai_answer(mode, elapsed_ms, *, degraded=False, cache_hit=False,
                     model="", product_count=0):
    with _LOCK:
        _AI_SAMPLES.append({
            "mode": str(mode or "")[:24],
            "elapsed_ms": max(0, int(elapsed_ms or 0)),
            "degraded": bool(degraded),
            "cache_hit": bool(cache_hit),
            "model": str(model or "")[:80],
            "product_count": max(0, int(product_count or 0)),
            "recorded_at": time.time(),
        })


def _series_summary(series):
    latencies = list(series.get("latencies") or [])
    requests = int(series.get("requests", 0) or 0)
    errors = int(series.get("errors", 0) or 0)
    return {
        "samples": len(latencies),
        "requests": requests,
        "errors": errors,
        "error_rate": round(errors / requests, 4) if requests else 0.0,
        "p50_ms": _percentile(latencies, 0.50),
        "p95_ms": _percentile(latencies, 0.95),
        "p99_ms": _percentile(latencies, 0.99),
        "max_ms": int(round(max(latencies))) if latencies else 0,
        "last_status": int(series.get("last_status", 0) or 0),
        "last_seen_at": float(series.get("last_seen_at", 0) or 0),
    }


def observability_snapshot():
    with _LOCK:
        routes = {
            key: _series_summary(series)
            for key, series in _REQUEST_SAMPLES.items()
        }
        ai_samples = list(_AI_SAMPLES)
    ai_by_mode = {}
    for mode in sorted({sample["mode"] for sample in ai_samples}):
        matching = [sample for sample in ai_samples if sample["mode"] == mode]
        latencies = [sample["elapsed_ms"] for sample in matching]
        ai_by_mode[mode] = {
            "samples": len(matching),
            "p50_ms": _percentile(latencies, 0.50),
            "p95_ms": _percentile(latencies, 0.95),
            "p99_ms": _percentile(latencies, 0.99),
            "degraded": sum(1 for sample in matching if sample["degraded"]),
            "cache_hits": sum(1 for sample in matching if sample["cache_hit"]),
            "last_model": next((
                sample["model"] for sample in reversed(matching)
                if sample["model"]
            ), ""),
        }
    memory = memory_snapshot()
    rss_mb = memory.get("rss_mb")
    documented_p95 = int(
        (ai_by_mode.get("documented") or {}).get("p95_ms", 0) or 0
    )
    status = "healthy"
    warnings = []
    if rss_mb is not None and float(rss_mb) >= 400:
        status = "critical"
        warnings.append("memory_near_instance_limit")
    elif rss_mb is not None and float(rss_mb) >= 320:
        status = "attention"
        warnings.append("memory_elevated")
    if documented_p95 >= 15000:
        status = "attention" if status == "healthy" else status
        warnings.append("documented_answers_slow")
    return {
        "status": status,
        "warnings": warnings,
        "uptime_seconds": max(0, int(time.time() - _STARTED_AT)),
        "pid": os.getpid(),
        "memory": memory,
        "routes": routes,
        "ai": ai_by_mode,
    }


def maybe_log_operational_warning(min_interval_seconds=60):
    """Emit one structured Render warning per interval when action is needed."""
    global _LAST_WARNING_LOG_AT
    now = time.time()
    with _LOCK:
        if now - _LAST_WARNING_LOG_AT < max(10, int(min_interval_seconds)):
            return
        _LAST_WARNING_LOG_AT = now
    snapshot = observability_snapshot()
    if not snapshot["warnings"]:
        return
    print(
        "[OPS] status={status} rss_mb={rss} warnings={warnings}".format(
            status=snapshot["status"],
            rss=snapshot["memory"].get("rss_mb"),
            warnings=",".join(snapshot["warnings"]),
        ),
        flush=True,
    )
