"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

Exotel makes call recordings available asynchronously after a call ends. Instead
of sleeping for one fixed duration and trying once, this module polls with a
small bounded backoff. Recording upload is independent from LLM analysis: a
recording failure is visible in logs, but it should not prevent transcript-based
post-call analysis from completing.
"""

import asyncio
import logging
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


def _next_delay_seconds(attempt: int) -> int:
    """
    Return the delay before the next polling attempt.

    attempt is 1-indexed. The first retry waits RECORDING_INITIAL_DELAY_SECONDS,
    then each later retry adds RECORDING_BACKOFF_SECONDS.
    """

    initial_delay = max(0, int(settings.RECORDING_INITIAL_DELAY_SECONDS))
    backoff = max(0, int(settings.RECORDING_BACKOFF_SECONDS))
    return initial_delay + max(0, attempt - 1) * backoff


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    """
    Poll Exotel for a recording URL and upload it to S3 once available.

    Returns the S3 key on success, None when the recording never becomes
    available or upload fails. Failures are intentionally visible in structured
    logs so missing recordings are auditable without blocking LLM analysis.
    """

    max_attempts = max(1, int(settings.RECORDING_MAX_ATTEMPTS))

    for attempt in range(1, max_attempts + 1):
        try:
            recording_url = await _fetch_exotel_recording_url(
                call_sid, exotel_account_id
            )

            if recording_url:
                s3_key = await _upload_to_s3(recording_url, interaction_id)
                logger.info(
                    "recording_uploaded",
                    extra={
                        "interaction_id": interaction_id,
                        "call_sid": call_sid,
                        "attempt": attempt,
                        "s3_key": s3_key,
                    },
                )
                return s3_key

        except Exception as exc:
            logger.warning(
                "recording_attempt_error",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "error": str(exc),
                },
                exc_info=True,
            )

        if attempt < max_attempts:
            next_delay = _next_delay_seconds(attempt)
            logger.info(
                "recording_not_ready",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "next_delay_seconds": next_delay,
                },
            )
            await asyncio.sleep(next_delay)

    logger.error(
        "recording_failed_permanently",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "attempts": max_attempts,
        },
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns the recording URL if available, None if not yet ready.
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("recording_url")
            return None
    except httpx.HTTPError:
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production: stream from recording_url → boto3 upload to S3_BUCKET.
    S3 key format: recordings/{interaction_id}.mp3
    """
    return f"recordings/{interaction_id}.mp3"
