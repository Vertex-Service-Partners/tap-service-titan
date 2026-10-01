"""Tests for the Vertex-specific ``call_recordings`` stream."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest.mock import Mock

import requests

from tap_service_titan.streams.telecom import (
    CallRecordingsStream,
    CallsStream,
    _duration_seconds,
)
from tap_service_titan.tap import TapServiceTitan

if TYPE_CHECKING:
    import pytest

_TENANT_ID = "3984754116"
_CONFIG = {
    "client_id": "x",
    "client_secret": "x",
    "st_app_key": "x",
    "tenant_id": _TENANT_ID,
    "call_recordings_stage": "ARCH_RAW.SERVICE_TITAN_TRANSCRIPTION.RECORDINGS",
}
_RECORDING_URL = f"/telecom/v2/tenant/{_TENANT_ID}/calls/7/recording"
_AUDIO = b"ID3abc"
_ONE_MINUTE_CALL = {
    "call_id": 7,
    "recording_url": _RECORDING_URL,
    "duration": "00:01:00",
    "created_on": None,
}


def _streams() -> tuple[CallsStream, CallRecordingsStream]:
    tap = TapServiceTitan(config=_CONFIG, parse_env_config=False)
    calls = tap.streams["calls"]
    recordings = tap.streams["call_recordings"]
    assert isinstance(calls, CallsStream)
    assert isinstance(recordings, CallRecordingsStream)
    return calls, recordings


def _recordings_with_response(
    monkeypatch: pytest.MonkeyPatch,
    response: Mock,
    *,
    existing: set[str] | None = None,
) -> CallRecordingsStream:
    """A ``call_recordings`` stream whose HTTP and stage access are stubbed out."""
    _, recordings = _streams()
    monkeypatch.setattr(recordings, "_existing_stage_files", Mock(return_value=existing or set()))
    # prepare_request would run the OAuth login; never touch the network in tests.
    monkeypatch.setattr(recordings, "prepare_request", Mock(return_value=Mock()))
    monkeypatch.setattr(recordings, "_request", Mock(return_value=response))
    return recordings


def test_duration_parsing() -> None:
    """ServiceTitan durations are HH:MM:SS with optional fractional seconds."""
    expected = {
        "00:00:37.0670506": 37.0670506,
        "00:00:17": 17,
        "01:02:03": 3723,
        None: None,
        "garbage": None,
    }
    assert {k: _duration_seconds(k) for k in expected} == expected


def test_calls_spawn_child_only_with_recording() -> None:
    """Calls without a recordingUrl must not spawn a child context (and must not warn)."""
    calls, _ = _streams()
    with_rec = {"id": 1, "recordingUrl": _RECORDING_URL, "duration": "00:01:00"}
    without = {"id": 2, "recordingUrl": None}
    assert list(calls.generate_child_contexts(with_rec, None)) == [
        {"call_id": 1, "recording_url": _RECORDING_URL, "duration": "00:01:00", "created_on": None}
    ]
    assert list(calls.generate_child_contexts(without, None)) == []


def test_child_is_wired_and_unpartitioned() -> None:
    """The child hangs off calls, keeps no per-call state, and hits the recording path."""
    _, recordings = _streams()
    assert recordings.parent_stream_type is CallsStream
    assert recordings.state_partitioning_keys == []
    assert recordings.get_url({"call_id": 42}).endswith(
        f"/telecom/v2/tenant/{_TENANT_ID}/calls/42/recording"
    )


def test_child_does_not_force_parent_full_table() -> None:
    """Selecting call_recordings must leave calls INCREMENTAL.

    Tap._set_compatible_replication_methods (run by sync_all) forces a parent to
    FULL_TABLE when a selected child ignores the parent replication key; that
    re-pulled every call on the first production run.
    """
    tap = TapServiceTitan(config=_CONFIG, parse_env_config=False)
    assert tap.streams["call_recordings"].selected
    tap._set_compatible_replication_methods()  # noqa: SLF001 - the exact SDK hook under test
    assert tap.streams["calls"].replication_method == "INCREMENTAL"


def test_404_is_not_an_error() -> None:
    """A 404 recording is a data point, not a fatal API error."""
    _, recordings = _streams()
    recordings.validate_response(Mock(spec=requests.Response, status_code=HTTPStatus.NOT_FOUND))


def test_short_calls_emit_nothing() -> None:
    """Calls under the minimum duration are neither downloaded nor recorded."""
    _, recordings = _streams()
    ctx = {**_ONE_MINUTE_CALL, "duration": "00:00:05"}
    assert list(recordings.get_records(ctx)) == []


def test_old_calls_are_skipped_by_lookback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calls older than call_recordings_lookback_days emit nothing — full refreshes stay bounded."""
    recordings = _recordings_with_response(
        monkeypatch, Mock(side_effect=AssertionError("must not download"))
    )
    ctx = {**_ONE_MINUTE_CALL, "created_on": "2020-01-01T00:00:00.1234567Z"}
    assert list(recordings.get_records(ctx)) == []


def test_existing_file_is_not_redownloaded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A file already on the stage is reported as downloaded without an HTTP call."""
    recordings = _recordings_with_response(
        monkeypatch, Mock(side_effect=AssertionError("must not download")), existing={"7.mp3"}
    )
    [rec] = list(recordings.get_records(_ONE_MINUTE_CALL))
    assert rec["status"] == "downloaded"
    assert rec["stagePath"] == f"{_TENANT_ID}/7.mp3"


def test_download_puts_to_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 200 with audio is PUT to the stage under <tenant>/<call_id>.<ext>."""
    response = Mock(
        spec=requests.Response,
        status_code=HTTPStatus.OK,
        content=_AUDIO,
        headers={"Content-Type": "audio/mpeg"},
    )
    recordings = _recordings_with_response(monkeypatch, response)
    put = Mock(return_value=f"{_TENANT_ID}/7.mp3")
    monkeypatch.setattr(recordings, "_put_to_stage", put)

    [rec] = list(recordings.get_records({**_ONE_MINUTE_CALL, "created_on": "2026-10-01T00:00:00Z"}))

    put.assert_called_once_with("7.mp3", _AUDIO)
    assert rec["status"] == "downloaded"
    assert rec["audioBytes"] == len(_AUDIO)
    assert rec["httpStatus"] == HTTPStatus.OK


def test_404_emits_no_recording(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 404 is recorded as no_recording so the call is never retried by downstream."""
    response = Mock(
        spec=requests.Response, status_code=HTTPStatus.NOT_FOUND, content=b"", headers={}
    )
    recordings = _recordings_with_response(monkeypatch, response)
    [rec] = list(recordings.get_records(_ONE_MINUTE_CALL))
    assert rec["status"] == "no_recording"
    assert rec["stagePath"] is None
