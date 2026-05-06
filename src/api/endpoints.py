"""
FastAPI endpoint for ending an interaction.

The endpoint is intentionally lightweight: load the interaction, mark it ended,
triage the transcript cheaply, and hand the full post-call flow to Celery. It no
longer fires downstream signal jobs with empty analysis for long calls.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from src.services.audit import audit_event
from src.services.triage import transcript_triage
from src.tasks.celery_tasks import process_interaction_end_background_task

logger = logging.getLogger(__name__)
router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
    background_tasks: BackgroundTasks,
):
    """End an interaction and enqueue durable post-call processing."""
    trace_id = str(uuid4())

    try:
        interaction = await _load_interaction(interaction_id)

        if not interaction:
            raise HTTPException(status_code=404, detail="Interaction not found")

        await _update_interaction_status(
            interaction_id=str(interaction_id),
            status="ENDED",
            ended_at=datetime.utcnow(),
            duration=request.duration_seconds,
            call_sid=request.call_sid,
        )

        transcript = interaction.get("conversation_data", {}).get("transcript", [])
        transcript_text = _build_transcript_text(transcript)
        triage_result = transcript_triage.triage(transcript)
        lane = triage_result.lane.value

        await audit_event(
            "postcall_received",
            interaction_id=str(interaction_id),
            customer_id=interaction["customer_id"],
            campaign_id=interaction["campaign_id"],
            session_id=str(session_id),
            trace_id=trace_id,
            lane=lane,
            detected_stage=triage_result.detected_stage,
        )
        await audit_event(
            "postcall_triaged",
            interaction_id=str(interaction_id),
            customer_id=interaction["customer_id"],
            campaign_id=interaction["campaign_id"],
            session_id=str(session_id),
            trace_id=trace_id,
            lane=lane,
            detected_stage=triage_result.detected_stage,
            reason=triage_result.reason,
            confidence=triage_result.confidence,
        )

        celery_payload = {
            "interaction_id": str(interaction_id),
            "session_id": str(session_id),
            "lead_id": interaction["lead_id"],
            "campaign_id": interaction["campaign_id"],
            "customer_id": interaction["customer_id"],
            "agent_id": interaction["agent_id"],
            "call_sid": request.call_sid,
            "transcript_text": transcript_text,
            "conversation_data": interaction.get("conversation_data", {}),
            "additional_data": request.additional_data or {},
            "ended_at": datetime.utcnow().isoformat(),
            "exotel_account_id": interaction.get("exotel_account_id"),
            "lane": lane,
            "detected_stage": triage_result.detected_stage,
            "triage_reason": triage_result.reason,
            "triage_confidence": triage_result.confidence,
            "trace_id": trace_id,
            "correlation_id": trace_id,
        }

        task = process_interaction_end_background_task.apply_async(
            args=[celery_payload],
            queue="postcall_processing",
        )

        logger.info(
            "postcall_enqueued",
            extra={
                "interaction_id": str(interaction_id),
                "customer_id": interaction["customer_id"],
                "campaign_id": interaction["campaign_id"],
                "session_id": str(session_id),
                "trace_id": trace_id,
                "lane": lane,
                "detected_stage": triage_result.detected_stage,
                "celery_task_id": task.id,
            },
        )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            message="Interaction ended, processing enqueued",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "end_interaction_failed",
            extra={
                "interaction_id": str(interaction_id),
                "trace_id": trace_id,
                "error": str(e),
            },
        )
        raise HTTPException(status_code=500, detail="Internal server error")


def _build_transcript_text(transcript: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
        for turn in transcript
    )


async def _load_interaction(interaction_id: UUID) -> Optional[Dict[str, Any]]:
    """Mock DB load for local development/tests."""
    return {
        "id": str(interaction_id),
        "lead_id": "mock-lead-id",
        "campaign_id": "mock-campaign-id",
        "customer_id": "mock-customer-id",
        "agent_id": "mock-agent-id",
        "exotel_account_id": "mock-exotel-account",
        "conversation_data": {
            "transcript": [
                {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
                {"role": "customer", "content": "Yes, speaking."},
                {"role": "agent", "content": "I'm calling from XYZ about your recent inquiry."},
                {"role": "customer", "content": "Oh yes, I was looking at the product."},
                {"role": "agent", "content": "Would you like to schedule a demo?"},
                {"role": "customer", "content": "Sure, let's do tomorrow at 3 PM."},
                {"role": "agent", "content": "Perfect, I've booked a demo for tomorrow at 3 PM."},
                {"role": "customer", "content": "Thank you, bye."},
            ]
        },
    }


async def _update_interaction_status(
    interaction_id: str,
    status: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """Mock DB update for local development/tests."""
    logger.info(
        "interaction_status_updated",
        extra={
            "interaction_id": interaction_id,
            "status": status,
            "ended_at": ended_at.isoformat(),
            "duration": duration,
            "call_sid": call_sid,
        },
    )
