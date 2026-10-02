"""Non-finite marketing stats from ServiceTitan's .NET serialiser are nulled, not fatal."""

from __future__ import annotations

from tap_service_titan.streams.marketing_ads import (
    AdGroupPerformanceStream,
    _null_non_finite,
)
from tap_service_titan.tap import TapServiceTitan

_CONFIG = {
    "client_id": "x",
    "client_secret": "x",
    "st_app_key": "x",
    "tenant_id": "3984754116",
}


def _adgroup_stream() -> AdGroupPerformanceStream:
    tap = TapServiceTitan(config=_CONFIG, parse_env_config=False)
    stream = tap.streams["adgroup_performance"]
    assert isinstance(stream, AdGroupPerformanceStream)
    stream.get_new_paginator()  # post_process stamps the current date window
    return stream


def test_null_non_finite() -> None:
    assert _null_non_finite("Infinity") is None
    assert _null_non_finite("-Infinity") is None
    assert _null_non_finite("NaN") is None
    assert _null_non_finite(float("inf")) is None
    assert _null_non_finite(float("nan")) is None
    assert _null_non_finite(0.25) == 0.25
    assert _null_non_finite(0) == 0
    assert _null_non_finite(None) is None
    assert _null_non_finite("infinity pools") == "infinity pools"


def test_post_process_nulls_infinite_rates() -> None:
    """Regression: clickRate = "Infinity" (clicks, zero impressions) killed the hourly run."""
    stream = _adgroup_stream()
    row = {
        "campaign": {"id": 11, "name": "Roof Repair"},
        "adGroup": {"id": 22, "name": "Emergency"},
        "keyword": None,
        "digitalStats": {
            "impressions": 0,
            "clicks": 3,
            "clickRate": "Infinity",
            "averageCPC": 1.5,
            "conversionRate": "NaN",
            "costPerConversion": None,
        },
        "leadStats": {"leads": 0, "bookingRate": "-Infinity", "avgTicket": 0.0},
        "returnOnInvestment": "Infinity",
    }

    out = stream.post_process(row)

    assert out is not None
    assert out["digitalStats"]["clickRate"] is None
    assert out["digitalStats"]["conversionRate"] is None
    assert out["digitalStats"]["costPerConversion"] is None
    assert out["digitalStats"]["averageCPC"] == 1.5
    assert out["digitalStats"]["clicks"] == 3
    assert out["leadStats"]["bookingRate"] is None
    assert out["leadStats"]["avgTicket"] == 0.0
    assert out["returnOnInvestment"] is None
    # the existing flattening still happens
    assert out["campaign_id"] == 11
    assert out["adGroup_id"] == 22
    assert out["keyword_id"] is None
