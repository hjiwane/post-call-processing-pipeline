import pytest

from src.services.llm_scheduler import InMemoryRateLimitBackend, LLMScheduler


@pytest.mark.asyncio
async def test_rate_limit_protection_defers_when_request_budget_unavailable():
    scheduler = LLMScheduler(
        backend=InMemoryRateLimitBackend(),
        window_seconds=60,
        global_requests_per_minute=2,
        global_tokens_per_minute=10_000,
        default_customer_tokens_per_minute=10_000,
        avg_tokens_per_call=100,
    )

    allowed = []
    for index in range(4):
        decision = await scheduler.reserve_budget(
            customer_id="customer-a",
            campaign_id="campaign-1",
            interaction_id=f"interaction-{index}",
            lane="cold",
            estimated_tokens=100,
        )
        allowed.append(decision)

    assert [decision.allowed for decision in allowed] == [True, True, False, False]
    assert allowed[2].reason == "global_request_limit_exhausted"
    assert allowed[2].retry_after_seconds > 0
    assert allowed[2].interaction_id == "interaction-2"


@pytest.mark.asyncio
async def test_per_customer_budget_isolated_between_customers():
    scheduler = LLMScheduler(
        backend=InMemoryRateLimitBackend(),
        window_seconds=60,
        global_requests_per_minute=10,
        global_tokens_per_minute=10_000,
        default_customer_tokens_per_minute=100,
        avg_tokens_per_call=60,
        hot_lane_borrow_enabled=False,
    )

    first_a = await scheduler.reserve_budget(
        customer_id="customer-a",
        campaign_id="campaign-1",
        interaction_id="interaction-a-1",
        lane="cold",
        estimated_tokens=60,
    )
    second_a = await scheduler.reserve_budget(
        customer_id="customer-a",
        campaign_id="campaign-1",
        interaction_id="interaction-a-2",
        lane="cold",
        estimated_tokens=60,
    )
    first_b = await scheduler.reserve_budget(
        customer_id="customer-b",
        campaign_id="campaign-2",
        interaction_id="interaction-b-1",
        lane="cold",
        estimated_tokens=60,
    )

    assert first_a.allowed is True
    assert second_a.allowed is False
    assert second_a.reason == "customer_token_budget_exhausted"
    assert second_a.retry_after_seconds > 0
    assert first_b.allowed is True


@pytest.mark.asyncio
async def test_short_or_skip_lane_never_reserves_quota():
    backend = InMemoryRateLimitBackend()
    scheduler = LLMScheduler(
        backend=backend,
        window_seconds=60,
        global_requests_per_minute=10,
        global_tokens_per_minute=10_000,
        default_customer_tokens_per_minute=10_000,
        avg_tokens_per_call=100,
    )

    decision = await scheduler.reserve_budget(
        customer_id="customer-a",
        campaign_id="campaign-1",
        interaction_id="short-interaction",
        lane="skip",
        estimated_tokens=100,
    )

    assert decision.allowed is False
    assert decision.retry_after_seconds == 0
    assert decision.reason == "skip_lane_no_llm_quota_reserved"

    follow_up = await scheduler.reserve_budget(
        customer_id="customer-a",
        campaign_id="campaign-1",
        interaction_id="normal-interaction",
        lane="cold",
        estimated_tokens=10_000,
    )
    assert follow_up.allowed is True
