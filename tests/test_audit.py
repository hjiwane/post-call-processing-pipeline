from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.services.post_call_processor import AnalysisResult
from src.tasks import celery_tasks


class DummyProcessor:
    async def process_post_call(self, ctx, single_prompt=True):
        return AnalysisResult(
            call_stage="rebook_confirmed",
            entities={},
            summary="Mocked analysis",
            raw_response={"call_stage": "rebook_confirmed"},
            tokens_used=111,
            latency_ms=5.0,
            provider="mock",
            model="mock-model",
        )


@pytest.mark.asyncio
async def test_full_flow_audit_events_include_interaction_id(monkeypatch, make_post_call_context):
    ctx = make_post_call_context("rebook_confirmed")
    captured_events = []

    async def capture_audit(event, **kwargs):
        captured_events.append((event, kwargs))

    monkeypatch.setattr(celery_tasks, "audit_event", capture_audit)
    monkeypatch.setattr(celery_tasks, "PostCallProcessor", lambda: DummyProcessor())
    monkeypatch.setattr(
        celery_tasks,
        "fetch_and_upload_recording",
        AsyncMock(return_value="recordings/test-interaction-001.mp3"),
    )
    monkeypatch.setattr(celery_tasks, "trigger_signal_jobs", AsyncMock())
    monkeypatch.setattr(celery_tasks, "update_lead_stage", AsyncMock())
    monkeypatch.setattr(celery_tasks.metrics_tracker, "track_processing_started", AsyncMock())
    monkeypatch.setattr(celery_tasks.metrics_tracker, "track_processing_completed", AsyncMock())

    payload = {
        "interaction_id": ctx.interaction_id,
        "session_id": ctx.session_id,
        "lead_id": ctx.lead_id,
        "campaign_id": ctx.campaign_id,
        "customer_id": ctx.customer_id,
        "agent_id": ctx.agent_id,
        "call_sid": ctx.call_sid,
        "transcript_text": ctx.transcript_text,
        "conversation_data": ctx.conversation_data,
        "additional_data": ctx.additional_data,
        "ended_at": ctx.ended_at.isoformat(),
        "exotel_account_id": ctx.exotel_account_id,
        "lane": "hot",
        "detected_stage": "rebook_confirmed",
        "triage_reason": "matched_hot_rule:rebook_confirmed",
        "triage_confidence": 0.9,
        "trace_id": "trace-1",
    }

    task = SimpleNamespace(request=SimpleNamespace(retries=0))
    await celery_tasks._process_interaction(task, payload)

    expected_stages = {
        "postcall_received",
        "postcall_triaged",
        "recording_poll_started",
        "recording_uploaded",
        "llm_analysis_started",
        "llm_budget_reserved",
        "llm_analysis_completed",
        "signal_jobs_completed",
        "lead_stage_updated",
        "postcall_completed",
    }
    captured_stage_names = {event for event, _ in captured_events}

    assert expected_stages.issubset(captured_stage_names)
    assert captured_events
    assert all(kwargs.get("interaction_id") == ctx.interaction_id for _, kwargs in captured_events)
