"""Celery tasks for post-call processing.

The task keeps the existing Celery/Redis stack but makes the expensive LLM path
rate-limit-aware and auditable. Recording polling runs independently from LLM
analysis so a missing recording does not delay or block transcript analysis.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from src.services.audit import audit_event
from src.services.metrics import metrics_tracker
from src.services.post_call_processor import (
    AnalysisResult,
    PostCallContext,
    PostCallProcessor,
    RateLimitDeferred,
)
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.triage import transcript_triage
from src.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    queue="postcall_processing",
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """Run post-call processing for one completed interaction."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_process_interaction(self, payload))
    except RateLimitDeferred as exc:
        interaction_id = payload.get("interaction_id")
        logger.info(
            "llm_budget_deferred",
            extra={
                "interaction_id": interaction_id,
                "customer_id": payload.get("customer_id"),
                "campaign_id": payload.get("campaign_id"),
                "trace_id": payload.get("trace_id"),
                "lane": payload.get("lane"),
                "retry_after_seconds": exc.retry_after_seconds,
                "reason": exc.reason,
                "estimated_tokens": exc.estimated_tokens,
                "attempt": self.request.retries,
            },
        )
        # Single retry path for rate-limit deferrals: Celery countdown only.
        # Do not enqueue into the Redis retry queue here, or the job can run twice.
        raise self.retry(exc=exc, countdown=max(1, int(exc.retry_after_seconds)))
    except Exception as exc:
        interaction_id = payload.get("interaction_id")
        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": interaction_id,
                "customer_id": payload.get("customer_id"),
                "campaign_id": payload.get("campaign_id"),
                "trace_id": payload.get("trace_id"),
                "lane": payload.get("lane"),
                "error": str(exc),
                "attempt": self.request.retries,
            },
        )
        # Avoid duplicate retry systems: Celery is the only retry mechanism here.
        raise self.retry(exc=exc)
    finally:
        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]) -> None:
    interaction_id = payload["interaction_id"]
    trace_id = payload.get("trace_id") or payload.get("correlation_id")
    lane = payload.get("lane")
    detected_stage = payload.get("detected_stage")

    ctx = _build_context(payload)

    if not lane or not detected_stage:
        triage_result = transcript_triage.triage(ctx.conversation_data or ctx.transcript_text)
        lane = triage_result.lane.value
        detected_stage = triage_result.detected_stage
        payload["lane"] = lane
        payload["detected_stage"] = detected_stage
        payload["triage_reason"] = triage_result.reason
        payload["triage_confidence"] = triage_result.confidence

    await metrics_tracker.track_processing_started(interaction_id)
    await audit_event(
        "postcall_received",
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        session_id=ctx.session_id,
        trace_id=trace_id,
        lane=lane,
        detected_stage=detected_stage,
        attempt=getattr(task.request, "retries", 0),
    )
    await audit_event(
        "postcall_triaged",
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        session_id=ctx.session_id,
        trace_id=trace_id,
        lane=lane,
        detected_stage=detected_stage,
        reason=payload.get("triage_reason"),
        confidence=payload.get("triage_confidence"),
    )

    recording_task = asyncio.create_task(_run_recording_pipeline(ctx, trace_id, lane))

    result: Optional[AnalysisResult] = None
    try:
        result = await _run_analysis_pipeline(ctx, trace_id, lane, detected_stage)
        await _run_downstream_pipeline(ctx, result, trace_id, lane, detected_stage)
        await audit_event(
            "postcall_completed",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=result.call_stage,
            tokens_used=result.tokens_used,
        )
    finally:
        # Recording is independent from analysis/downstream actions, but we still
        # await it before task completion so success/failure is auditable.
        await recording_task


async def _run_recording_pipeline(
    ctx: PostCallContext,
    trace_id: Optional[str],
    lane: str,
) -> Optional[str]:
    await audit_event(
        "recording_poll_started",
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        session_id=ctx.session_id,
        trace_id=trace_id,
        lane=lane,
        call_sid=ctx.call_sid,
    )

    try:
        s3_key = await fetch_and_upload_recording(
            interaction_id=ctx.interaction_id,
            call_sid=ctx.call_sid,
            exotel_account_id=ctx.exotel_account_id or "",
        )
        if s3_key:
            await audit_event(
                "recording_uploaded",
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                session_id=ctx.session_id,
                trace_id=trace_id,
                lane=lane,
                call_sid=ctx.call_sid,
                s3_key=s3_key,
            )
        else:
            await audit_event(
                "recording_failed_permanently",
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                session_id=ctx.session_id,
                trace_id=trace_id,
                lane=lane,
                call_sid=ctx.call_sid,
                status="failed",
                level="error",
            )
        return s3_key
    except Exception as exc:  # Defensive: recording must not block analysis.
        logger.exception(
            "recording_pipeline_failed",
            extra={
                "interaction_id": ctx.interaction_id,
                "customer_id": ctx.customer_id,
                "campaign_id": ctx.campaign_id,
                "trace_id": trace_id,
                "lane": lane,
                "call_sid": ctx.call_sid,
                "error": str(exc),
            },
        )
        await audit_event(
            "recording_failed_permanently",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            call_sid=ctx.call_sid,
            status="failed",
            level="error",
            error=str(exc),
        )
        return None


async def _run_analysis_pipeline(
    ctx: PostCallContext,
    trace_id: Optional[str],
    lane: str,
    detected_stage: Optional[str],
) -> AnalysisResult:
    await audit_event(
        "llm_analysis_started",
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        session_id=ctx.session_id,
        trace_id=trace_id,
        lane=lane,
        detected_stage=detected_stage,
    )

    processor = PostCallProcessor()
    try:
        result = await processor.process_post_call(ctx, single_prompt=True)
    except RateLimitDeferred as exc:
        await audit_event(
            "llm_budget_deferred",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=detected_stage,
            status="deferred",
            retry_after_seconds=exc.retry_after_seconds,
            reason=exc.reason,
            estimated_tokens=exc.estimated_tokens,
        )
        raise

    if result.tokens_used > 0:
        await audit_event(
            "llm_budget_reserved",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=result.call_stage,
            actual_tokens=result.tokens_used,
        )

    await metrics_tracker.track_processing_completed(
        ctx.interaction_id,
        result.tokens_used,
        result.latency_ms,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        lane=lane,
    )
    await audit_event(
        "llm_analysis_completed",
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        session_id=ctx.session_id,
        trace_id=trace_id,
        lane=lane,
        detected_stage=result.call_stage,
        actual_tokens=result.tokens_used,
        latency_ms=result.latency_ms,
    )
    return result


async def _run_downstream_pipeline(
    ctx: PostCallContext,
    result: AnalysisResult,
    trace_id: Optional[str],
    lane: str,
    detected_stage: Optional[str],
) -> None:
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
        )
        await audit_event(
            "signal_jobs_completed",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=result.call_stage,
        )
    except Exception as exc:
        logger.warning(
            "signal_jobs_failed",
            extra={
                "interaction_id": ctx.interaction_id,
                "customer_id": ctx.customer_id,
                "campaign_id": ctx.campaign_id,
                "trace_id": trace_id,
                "lane": lane,
                "error": str(exc),
            },
            exc_info=True,
        )
        await audit_event(
            "signal_jobs_failed",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=detected_stage,
            status="failed",
            level="warning",
            error=str(exc),
        )

    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
        )
        await audit_event(
            "lead_stage_updated",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=result.call_stage,
            lead_id=ctx.lead_id,
        )
    except Exception as exc:
        logger.warning(
            "lead_stage_failed",
            extra={
                "interaction_id": ctx.interaction_id,
                "customer_id": ctx.customer_id,
                "campaign_id": ctx.campaign_id,
                "trace_id": trace_id,
                "lane": lane,
                "error": str(exc),
            },
            exc_info=True,
        )
        await audit_event(
            "lead_stage_failed",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            trace_id=trace_id,
            lane=lane,
            detected_stage=detected_stage,
            status="failed",
            level="warning",
            error=str(exc),
        )


def _build_context(payload: Dict[str, Any]) -> PostCallContext:
    return PostCallContext(
        interaction_id=payload["interaction_id"],
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
    )
