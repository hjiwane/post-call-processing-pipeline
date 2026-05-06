"""Lightweight structured audit logging for post-call processing.

For the assessment this writes structured log events only. In production this
same helper can be backed by a durable table without changing call sites.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


async def audit_event(
    event: str,
    *,
    interaction_id: str,
    customer_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    session_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    lane: Optional[str] = None,
    detected_stage: Optional[str] = None,
    status: str = "ok",
    level: str = "info",
    **extra: Any,
) -> None:
    """Emit a consistent audit event with correlation fields.

    The function is async so callers can later persist to Postgres without
    refactoring the Celery pipeline. Today it remains local-test friendly.
    """
    payload = {
        "interaction_id": interaction_id,
        "customer_id": customer_id,
        "campaign_id": campaign_id,
        "session_id": session_id,
        "trace_id": trace_id,
        "lane": lane,
        "detected_stage": detected_stage,
        "status": status,
        **extra,
    }
    # Avoid cluttering logs with null values while keeping required IDs present
    # when they are supplied by the endpoint/Celery payload.
    payload = {key: value for key, value in payload.items() if value is not None}

    log_fn = getattr(logger, level, logger.info)
    log_fn(event, extra=payload)
