"""Telecom streams for the ServiceTitan tap."""

from __future__ import annotations

import base64
import contextlib
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th

from tap_service_titan.client import ServiceTitanBaseStream, ServiceTitanExportStream
from tap_service_titan.openapi_specs import TELECOM, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context, Record


class _BaseTelecomExportStream(ServiceTitanExportStream, api_prefix="/telecom/v2"):
    pass


class CallsStream(_BaseTelecomExportStream):
    """Define calls stream."""

    name = "calls"
    path = "/export/calls"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(TELECOM, key="Telecom.V2.ExportCallResponse")

    @override
    def generate_child_contexts(
        self,
        record: Record,
        context: Context | None,
    ) -> Iterable[Context | None]:
        """Spawn the ``call_recordings`` child only for calls that have a recording.

        Yielding nothing (rather than returning ``None`` from ``get_child_context``)
        skips the child without the SDK logging a warning per recording-less call.
        """
        if record.get("recordingUrl"):
            yield {
                "call_id": record["id"],
                "recording_url": record["recordingUrl"],
                "duration": record.get("duration"),
                "created_on": record.get("createdOn"),
            }


def _duration_seconds(duration: str | None) -> float | None:
    """Parse ServiceTitan's ``HH:MM:SS[.fffffff]`` call duration into seconds."""
    if not duration:
        return None
    try:
        hours, minutes, seconds = duration.split(":")
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    except ValueError:
        return None


_ISO_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})")


def _parse_timestamp(value: str | None) -> datetime | None:
    """Second-precision parse of ServiceTitan's ISO timestamps (7-digit fractions, ``Z``)."""
    match = _ISO_PREFIX.match(value or "")
    if not match:
        return None
    return datetime.fromisoformat(f"{match.group(1)}T{match.group(2)}").replace(tzinfo=timezone.utc)


def _env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None:
            return value
    return None


def _snowflake_private_key(*names: str) -> bytes | None:
    """Return PKCS#8 DER bytes from the first env var set, accepting b64 PEM, b64 DER or PEM."""
    value = _env(*names)
    if value is None:
        return None
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415 - optional extra

    text = value.strip()
    raw = text.encode() if text.startswith("-----BEGIN") else base64.b64decode(text)
    load = (
        serialization.load_pem_private_key
        if raw.lstrip().startswith(b"-----BEGIN")
        else serialization.load_der_private_key
    )
    key = load(raw, password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class CallRecordingsStream(ServiceTitanBaseStream, api_prefix="/telecom/v2"):
    """Download each call's recording to a Snowflake internal stage.

    Vertex-specific stream (not upstream). Child of ``calls``: for every call
    with a ``recordingUrl`` it GETs ``/calls/{id}/recording``, ``PUT``s the audio
    to ``{call_recordings_stage}/{tenant_id}/{call_id}.<ext>`` and emits one
    metadata record with the resulting ``stagePath``. Downstream dbt runs
    Cortex ``AI_TRANSCRIBE`` against that path.

    The Snowflake connection reuses the loader's environment
    (``TARGET_SNOWFLAKE_ACCOUNT/USER/ROLE/WAREHOUSE`` + ``TARGET_SNOWFLAKE_PRIVATE_KEY``,
    falling back to the ``DBT_SNOWFLAKE_*`` names and to ``TARGET_SNOWFLAKE_PASSWORD``)
    so no extra credentials are configured. Files already on the stage are not
    re-downloaded (one ``LIST`` per sync), so re-emitted parent calls are cheap.
    """

    name = "call_recordings"
    path = "/calls/{call_id}/recording"
    primary_keys = ("id",)
    replication_key = None
    parent_stream_type = CallsStream
    ignore_parent_replication_key = True
    # One parent context per call — never partition state by it.
    state_partitioning_keys: ClassVar[list[str]] = []

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="ServiceTitan call id."),
        th.Property("recordingUrl", th.StringType),
        th.Property(
            "stagePath",
            th.StringType,
            description="Path of the audio file relative to the stage root, or null.",
        ),
        th.Property(
            "status",
            th.StringType,
            description="downloaded | no_recording (404 / empty body) | too_large.",
        ),
        th.Property("httpStatus", th.IntegerType),
        th.Property("contentType", th.StringType),
        th.Property("audioBytes", th.IntegerType),
        th.Property("durationSeconds", th.NumberType),
        th.Property("callCreatedOn", th.DateTimeType),
        th.Property("fetchedAt", th.DateTimeType),
    ).to_dict()

    # AI_TRANSCRIBE caps a file at 700 MB; stay well under it.
    MAX_AUDIO_BYTES = 650 * 1024 * 1024

    # --- Snowflake stage helpers -------------------------------------------------

    @property
    def _stage(self) -> str:
        stage: str = self.config["call_recordings_stage"]
        return stage if stage.startswith("@") else f"@{stage}"

    def _snowflake_conn(self):  # noqa: ANN202
        cached = getattr(self, "_sf_conn_cache", None)
        if cached is not None:
            return cached
        import snowflake.connector  # noqa: PLC0415 - optional extra, only when syncing

        kwargs: dict[str, Any] = {
            "account": _env("TARGET_SNOWFLAKE_ACCOUNT", "DBT_SNOWFLAKE_ACCOUNT"),
            "user": _env("TARGET_SNOWFLAKE_USER", "DBT_SNOWFLAKE_USER"),
            "role": _env("TARGET_SNOWFLAKE_ROLE", "DBT_SNOWFLAKE_ROLE"),
            "warehouse": _env("TARGET_SNOWFLAKE_WAREHOUSE", "DBT_SNOWFLAKE_WAREHOUSE"),
        }
        key = _snowflake_private_key("TARGET_SNOWFLAKE_PRIVATE_KEY", "DBT_SNOWFLAKE_PRIVATE_KEY")
        if key is not None:
            kwargs["private_key"] = key
        else:
            kwargs["password"] = _env("TARGET_SNOWFLAKE_PASSWORD", "DBT_SNOWFLAKE_PASSWORD")
        missing = [k for k, v in kwargs.items() if v is None]
        if missing:
            msg = (
                "call_recordings needs the loader's Snowflake env "
                f"(TARGET_SNOWFLAKE_* / DBT_SNOWFLAKE_*); missing: {', '.join(missing)}"
            )
            raise RuntimeError(msg)
        cached = snowflake.connector.connect(**kwargs)
        self._sf_conn_cache = cached
        return cached

    def _existing_stage_files(self) -> set[str]:
        """Basenames already under this tenant's stage prefix (one LIST per sync)."""
        cached = getattr(self, "_stage_files_cache", None)
        if cached is not None:
            return cached
        cur = self._snowflake_conn().cursor()
        try:
            cur.execute(f"LIST {self._stage}/{self.tenant_id}/")
            names = {str(row[0]).rsplit("/", 1)[-1] for row in cur.fetchall()}
        finally:
            cur.close()
        self._stage_files_cache = names
        return names

    def _put_to_stage(self, basename: str, content: bytes) -> str:
        """PUT ``content`` as ``{tenant_id}/{basename}``; return the relative stage path."""
        # PUT keeps the local basename, so the temp file's name IS the stage filename.
        tmpdir = Path(tempfile.mkdtemp())
        local_path = tmpdir / basename
        try:
            local_path.write_bytes(content)
            # AUTO_COMPRESS=FALSE: AI_TRANSCRIBE needs the raw audio, not a .gz.
            sql = (
                f"PUT 'file://{local_path}' '{self._stage}/{self.tenant_id}' "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            cur = self._snowflake_conn().cursor()
            try:
                cur.execute(sql)
            finally:
                cur.close()
        finally:
            with contextlib.suppress(OSError):
                local_path.unlink()
            with contextlib.suppress(OSError):
                tmpdir.rmdir()
        self._existing_stage_files().add(basename)
        return f"{self.tenant_id}/{basename}"

    # --- HTTP ----------------------------------------------------------------------

    @override
    def validate_response(self, response: requests.Response) -> None:
        """A 404 means ServiceTitan has no audio for this call — a data point, not an error."""
        if response.status_code == HTTPStatus.NOT_FOUND:
            return
        super().validate_response(response)

    @staticmethod
    def _extension(content_type: str | None) -> str:
        ctype = (content_type or "").lower()
        if "wav" in ctype:
            return "wav"
        if "mp4" in ctype or "m4a" in ctype:
            return "m4a"
        return "mp3"

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Fetch one recording for the parent call and emit its metadata."""
        if context is None:  # pragma: no cover - the SDK always passes the parent context
            return
        call_id = context["call_id"]
        duration = _duration_seconds(context.get("duration"))
        min_duration = self.config.get("call_recordings_min_duration_seconds", 20)
        if duration is not None and duration < min_duration:
            return  # too short to be worth transcribing; emit nothing

        # A full refresh (or a parent bookmark reset) replays every call since
        # start_date; without this cap that is every recording the tenant ever
        # made — hours of PUTs and a transcription bill to match.
        lookback_days = self.config.get("call_recordings_lookback_days", 30)
        created_on = _parse_timestamp(context.get("created_on"))
        if (
            lookback_days
            and created_on is not None
            and created_on < datetime.now(tz=timezone.utc) - timedelta(days=lookback_days)
        ):
            return

        base: dict[str, Any] = {
            "id": call_id,
            "recordingUrl": context["recording_url"],
            "stagePath": None,
            "status": None,
            "httpStatus": None,
            "contentType": None,
            "audioBytes": None,
            "durationSeconds": duration,
            "callCreatedOn": context.get("created_on"),
            "fetchedAt": datetime.now(tz=timezone.utc).isoformat(),
        }

        existing = self._existing_stage_files()
        for ext in ("mp3", "m4a", "wav"):
            if f"{call_id}.{ext}" in existing:
                base["stagePath"] = f"{self.tenant_id}/{call_id}.{ext}"
                base["status"] = "downloaded"
                yield base
                return

        response = self._request(self.prepare_request(context, next_page_token=None), context)
        base["httpStatus"] = response.status_code
        base["contentType"] = response.headers.get("Content-Type")
        if response.status_code == HTTPStatus.NOT_FOUND or not response.content:
            base["status"] = "no_recording"
            yield base
            return

        base["audioBytes"] = len(response.content)
        if len(response.content) > self.MAX_AUDIO_BYTES:
            base["status"] = "too_large"
            yield base
            return

        basename = f"{call_id}.{self._extension(base['contentType'])}"
        base["stagePath"] = self._put_to_stage(basename, response.content)
        base["status"] = "downloaded"
        yield base
