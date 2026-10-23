"""Tests for stripping inline base64 images from ``jobs.summary``."""

from __future__ import annotations

from tap_service_titan.streams.jpm import _strip_inline_data_uris


def test_strips_inline_base64_image() -> None:
    """An inline data URI is replaced, surrounding HTML kept."""
    html = '<p>Roof <img src="data:image/png;base64,iVBORw0KGgo+/AAA=="> done</p>'
    expected = '<p>Roof <img src="data:,inline-image-removed"> done</p>'
    assert _strip_inline_data_uris(html) == expected


def test_leaves_plain_summary_untouched() -> None:
    """A summary without inline images passes through as-is."""
    html = '<p>Replace 3 shingles, <a href="https://example.com/x.png">photo</a></p>'
    assert _strip_inline_data_uris(html) is html


def test_handles_missing_summary() -> None:
    """Null and empty summaries pass through."""
    assert _strip_inline_data_uris(None) is None
    assert _strip_inline_data_uris("") == ""
