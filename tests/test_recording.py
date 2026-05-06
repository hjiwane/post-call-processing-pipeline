import inspect
import logging
from unittest.mock import AsyncMock

import pytest

from src.services import recording


@pytest.mark.asyncio
async def test_recording_available_after_delayed_attempts_eventually_uploads(monkeypatch, caplog):
    fetch = AsyncMock(side_effect=[None, None, "https://example.com/recording.mp3"])
    upload = AsyncMock(return_value="recordings/interaction-1.mp3")
    sleep = AsyncMock()

    monkeypatch.setattr(recording, "_fetch_exotel_recording_url", fetch)
    monkeypatch.setattr(recording, "_upload_to_s3", upload)
    monkeypatch.setattr(recording.asyncio, "sleep", sleep)
    monkeypatch.setattr(recording.settings, "RECORDING_MAX_ATTEMPTS", 5)
    monkeypatch.setattr(recording.settings, "RECORDING_INITIAL_DELAY_SECONDS", 1)
    monkeypatch.setattr(recording.settings, "RECORDING_BACKOFF_SECONDS", 2)

    with caplog.at_level(logging.INFO):
        result = await recording.fetch_and_upload_recording(
            interaction_id="interaction-1",
            call_sid="call-1",
            exotel_account_id="account-1",
        )

    assert result == "recordings/interaction-1.mp3"
    assert fetch.await_count == 3
    upload.assert_awaited_once_with("https://example.com/recording.mp3", "interaction-1")
    assert sleep.await_count == 2
    assert [call.args[0] for call in sleep.await_args_list] == [1, 3]
    assert "recording_not_ready" in caplog.text
    assert "recording_uploaded" in caplog.text


@pytest.mark.asyncio
async def test_recording_unavailable_after_max_attempts_logs_and_returns_failure(monkeypatch, caplog):
    fetch = AsyncMock(return_value=None)
    upload = AsyncMock()
    sleep = AsyncMock()

    monkeypatch.setattr(recording, "_fetch_exotel_recording_url", fetch)
    monkeypatch.setattr(recording, "_upload_to_s3", upload)
    monkeypatch.setattr(recording.asyncio, "sleep", sleep)
    monkeypatch.setattr(recording.settings, "RECORDING_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(recording.settings, "RECORDING_INITIAL_DELAY_SECONDS", 2)
    monkeypatch.setattr(recording.settings, "RECORDING_BACKOFF_SECONDS", 4)

    with caplog.at_level(logging.INFO):
        result = await recording.fetch_and_upload_recording(
            interaction_id="interaction-2",
            call_sid="call-2",
            exotel_account_id="account-2",
        )

    assert result is None
    assert fetch.await_count == 3
    upload.assert_not_called()
    assert sleep.await_count == 2
    assert [call.args[0] for call in sleep.await_args_list] == [2, 6]
    assert "recording_not_ready" in caplog.text
    assert "recording_failed_permanently" in caplog.text


def test_no_fixed_45_second_sleep_remains():
    source = inspect.getsource(recording.fetch_and_upload_recording)

    assert "RECORDING_WAIT_SECONDS" not in source
    assert "sleep(45" not in source
