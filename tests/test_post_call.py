import pytest
from unittest.mock import AsyncMock, patch

from src.services.llm_scheduler import LLMScheduleDecision
from src.services.post_call_processor import PostCallProcessor, RateLimitDeferred


class FakeScheduler:
    def __init__(self, *, allowed: bool = True, reason: str = "allowed"):
        self.allowed = allowed
        self.reason = reason
        self.reserve_calls = []
        self.usage_records = []

    async def reserve_budget(self, **kwargs):
        self.reserve_calls.append(kwargs)
        return LLMScheduleDecision(
            allowed=self.allowed,
            retry_after_seconds=30 if not self.allowed else 0,
            reason=self.reason,
            customer_id=kwargs["customer_id"],
            campaign_id=kwargs["campaign_id"],
            interaction_id=kwargs["interaction_id"],
            estimated_tokens=kwargs["estimated_tokens"],
        )

    async def record_actual_usage(self, **kwargs):
        self.usage_records.append(kwargs)
        return kwargs


@pytest.mark.asyncio
async def test_quota_unavailable_means_llm_is_not_called(make_post_call_context):
    ctx = make_post_call_context("rebook_confirmed")
    scheduler = FakeScheduler(allowed=False, reason="global_token_limit_exhausted")
    processor = PostCallProcessor(scheduler=scheduler)

    with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
        with pytest.raises(RateLimitDeferred) as exc_info:
            await processor.process_post_call(ctx)

    mock_llm.assert_not_called()
    assert exc_info.value.retry_after_seconds == 30
    assert exc_info.value.reason == "global_token_limit_exhausted"
    assert scheduler.reserve_calls[0]["interaction_id"] == ctx.interaction_id
    assert scheduler.usage_records == []


@pytest.mark.asyncio
async def test_quota_available_means_llm_is_called_once(make_post_call_context):
    ctx = make_post_call_context("rebook_confirmed")
    scheduler = FakeScheduler(allowed=True)
    processor = PostCallProcessor(scheduler=scheduler)

    with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = {
            "call_stage": "rebook_confirmed",
            "entities": {},
            "summary": "Customer confirmed rebooking.",
            "usage": {"total_tokens": 1234},
        }
        with patch.object(processor, "_update_interaction_metadata", new_callable=AsyncMock):
            result = await processor.process_post_call(ctx)

    mock_llm.assert_called_once()
    assert result.call_stage == "rebook_confirmed"
    assert result.tokens_used == 1234
    assert scheduler.reserve_calls[0]["customer_id"] == ctx.customer_id
    assert scheduler.reserve_calls[0]["campaign_id"] == ctx.campaign_id
    assert scheduler.reserve_calls[0]["interaction_id"] == ctx.interaction_id
    assert scheduler.reserve_calls[0]["lane"] == "hot"


@pytest.mark.asyncio
async def test_actual_tokens_are_recorded_after_successful_response(make_post_call_context):
    ctx = make_post_call_context("demo_booked")
    scheduler = FakeScheduler(allowed=True)
    processor = PostCallProcessor(scheduler=scheduler)

    with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = {
            "call_stage": "demo_booked",
            "entities": {"demo_time": "Thursday 11 AM"},
            "summary": "Demo booked.",
            "usage": {"total_tokens": 1400},
        }
        with patch.object(processor, "_update_interaction_metadata", new_callable=AsyncMock):
            await processor.process_post_call(ctx)

    assert len(scheduler.usage_records) == 1
    usage = scheduler.usage_records[0]
    assert usage["customer_id"] == ctx.customer_id
    assert usage["campaign_id"] == ctx.campaign_id
    assert usage["interaction_id"] == ctx.interaction_id
    assert usage["lane"] == "hot"
    assert usage["actual_tokens"] == 1400
    assert usage["estimated_tokens"] == scheduler.reserve_calls[0]["estimated_tokens"]


@pytest.mark.asyncio
async def test_short_transcript_skips_scheduler_and_llm(make_post_call_context):
    ctx = make_post_call_context("short_call_hangup")
    scheduler = FakeScheduler(allowed=True)
    processor = PostCallProcessor(scheduler=scheduler)

    with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
        with patch.object(processor, "_update_interaction_metadata", new_callable=AsyncMock):
            result = await processor.process_post_call(ctx)

    mock_llm.assert_not_called()
    assert scheduler.reserve_calls == []
    assert scheduler.usage_records == []
    assert result.tokens_used == 0
    assert result.call_stage in {"short_call", "wrong_number"}

@pytest.mark.asyncio
async def test_real_scheduler_rate_limit_defers_before_second_llm_call(make_post_call_context):
    from src.services.llm_scheduler import InMemoryRateLimitBackend, LLMScheduler

    scheduler = LLMScheduler(
        backend=InMemoryRateLimitBackend(),
        window_seconds=60,
        global_requests_per_minute=1,
        global_tokens_per_minute=10_000,
        default_customer_tokens_per_minute=10_000,
        avg_tokens_per_call=100,
    )
    processor = PostCallProcessor(scheduler=scheduler)
    first_ctx = make_post_call_context("rebook_confirmed", interaction_id="interaction-1")
    second_ctx = make_post_call_context("demo_booked", interaction_id="interaction-2")

    with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
        mock_llm.return_value = {
            "call_stage": "rebook_confirmed",
            "entities": {},
            "summary": "Customer confirmed rebooking.",
            "usage": {"total_tokens": 100},
        }
        with patch.object(processor, "_update_interaction_metadata", new_callable=AsyncMock):
            await processor.process_post_call(first_ctx)
            with pytest.raises(RateLimitDeferred) as exc_info:
                await processor.process_post_call(second_ctx)

    assert mock_llm.await_count == 1
    assert exc_info.value.retry_after_seconds > 0
    assert exc_info.value.reason == "global_request_limit_exhausted"
