"""
Rate-limit-aware LLM scheduling for post-call processing.

This module deliberately keeps the model simple: fixed 60-second windows,
Redis-like counters, and explicit reservation before any LLM call is made.
It is designed to be testable with the in-memory backend below, while also
working with redis.asyncio clients that expose get/incrby/expire/ttl.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from src.config import settings

try:
    from src.utils.redis_client import redis_client as _redis_client
except Exception:  # pragma: no cover - keeps tests local when redis is not installed
    _redis_client = None

logger = logging.getLogger(__name__)


@runtime_checkable
class RateLimitBackend(Protocol):
    """Small Redis-like surface needed by the scheduler."""

    async def get(self, key: str) -> Optional[str]:
        ...

    async def incrby(self, key: str, amount: int) -> int:
        ...

    async def expire(self, key: str, seconds: int) -> object:
        ...

    async def ttl(self, key: str) -> int:
        ...


class InMemoryRateLimitBackend:
    """
    Async Redis-like backend for unit tests and local use.

    It supports only the operations the scheduler needs. Expiry is lazy: keys
    are removed when read or mutated after their expiry time has passed.
    """

    def __init__(self) -> None:
        self._values: dict[str, int] = {}
        self._expires_at: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[str]:
        async with self._lock:
            self._purge_if_expired(key)
            if key not in self._values:
                return None
            return str(self._values[key])

    async def incrby(self, key: str, amount: int) -> int:
        async with self._lock:
            self._purge_if_expired(key)
            self._values[key] = self._values.get(key, 0) + int(amount)
            return self._values[key]

    async def expire(self, key: str, seconds: int) -> bool:
        async with self._lock:
            self._purge_if_expired(key)
            if key not in self._values:
                return False
            self._expires_at[key] = time.time() + seconds
            return True

    async def ttl(self, key: str) -> int:
        async with self._lock:
            self._purge_if_expired(key)
            if key not in self._values:
                return -2
            expires_at = self._expires_at.get(key)
            if expires_at is None:
                return -1
            return max(0, int(expires_at - time.time()))

    def _purge_if_expired(self, key: str) -> None:
        expires_at = self._expires_at.get(key)
        if expires_at is not None and expires_at <= time.time():
            self._values.pop(key, None)
            self._expires_at.pop(key, None)


@dataclass(frozen=True)
class LLMScheduleDecision:
    allowed: bool
    retry_after_seconds: int
    reason: str
    customer_id: str
    campaign_id: str
    interaction_id: str
    estimated_tokens: int


@dataclass(frozen=True)
class LLMUsageRecord:
    customer_id: str
    campaign_id: str
    interaction_id: str
    lane: str
    estimated_tokens: int
    actual_tokens: int


class LLMScheduler:
    """
    Fixed-window scheduler for deciding whether an LLM call may run now.

    The scheduler reserves request and token capacity before the caller invokes
    the LLM. If capacity is unavailable, the returned decision has
    allowed=False and the caller must not call the LLM.
    """

    SKIP_LANES = {"skip", "short"}
    HOT_LANE = "hot"
    COLD_LANE = "cold"

    def __init__(
        self,
        backend: Optional[RateLimitBackend] = None,
        *,
        window_seconds: Optional[int] = None,
        global_requests_per_minute: Optional[int] = None,
        global_tokens_per_minute: Optional[int] = None,
        default_customer_tokens_per_minute: Optional[int] = None,
        avg_tokens_per_call: Optional[int] = None,
        hot_lane_borrow_enabled: Optional[bool] = None,
    ) -> None:
        self.backend = backend or _redis_client or InMemoryRateLimitBackend()
        self.window_seconds = window_seconds or settings.LLM_RATE_LIMIT_WINDOW_SECONDS
        self.global_requests_per_window = (
            global_requests_per_minute or settings.LLM_REQUESTS_PER_MINUTE
        )
        self.global_tokens_per_window = (
            global_tokens_per_minute or settings.LLM_TOKENS_PER_MINUTE
        )
        self.default_customer_tokens_per_window = (
            default_customer_tokens_per_minute
            or settings.CUSTOMER_DEFAULT_TOKENS_PER_MINUTE
        )
        self.avg_tokens_per_call = avg_tokens_per_call or settings.LLM_AVG_TOKENS_PER_CALL
        self.hot_lane_borrow_enabled = (
            settings.LLM_HOT_LANE_BORROW_ENABLED
            if hot_lane_borrow_enabled is None
            else hot_lane_borrow_enabled
        )

    async def reserve_budget(
        self,
        *,
        customer_id: str,
        campaign_id: str,
        interaction_id: str,
        lane: str = COLD_LANE,
        estimated_tokens: Optional[int] = None,
        customer_tokens_per_minute: Optional[int] = None,
    ) -> LLMScheduleDecision:
        """
        Reserve LLM request/token budget for one potential LLM call.

        Returns allowed=False when the caller must defer or skip. This function
        does not call the LLM and does not know anything about prompt contents.
        """
        normalized_lane = lane.lower().strip()
        tokens = max(1, int(estimated_tokens or self.avg_tokens_per_call))

        if normalized_lane in self.SKIP_LANES:
            decision = LLMScheduleDecision(
                allowed=False,
                retry_after_seconds=0,
                reason="skip_lane_no_llm_quota_reserved",
                customer_id=customer_id,
                campaign_id=campaign_id,
                interaction_id=interaction_id,
                estimated_tokens=tokens,
            )
            self._log_deferred(decision, lane=normalized_lane)
            return decision

        window_id = self._current_window_id()
        keys = self._keys(window_id, customer_id)
        customer_limit = int(
            customer_tokens_per_minute or self.default_customer_tokens_per_window
        )

        global_requests_used = await self._read_int(keys["global_requests"])
        global_tokens_used = await self._read_int(keys["global_tokens"])
        customer_tokens_used = await self._read_int(keys["customer_tokens"])

        retry_after = await self._retry_after_seconds(keys["global_requests"])

        if global_requests_used + 1 > self.global_requests_per_window:
            return self._defer(
                customer_id=customer_id,
                campaign_id=campaign_id,
                interaction_id=interaction_id,
                estimated_tokens=tokens,
                retry_after_seconds=retry_after,
                reason="global_request_limit_exhausted",
                lane=normalized_lane,
            )

        if global_tokens_used + tokens > self.global_tokens_per_window:
            return self._defer(
                customer_id=customer_id,
                campaign_id=campaign_id,
                interaction_id=interaction_id,
                estimated_tokens=tokens,
                retry_after_seconds=retry_after,
                reason="global_token_limit_exhausted",
                lane=normalized_lane,
            )

        customer_budget_exhausted = customer_tokens_used + tokens > customer_limit
        hot_lane_can_borrow = (
            normalized_lane == self.HOT_LANE
            and self.hot_lane_borrow_enabled
            and global_tokens_used + tokens <= self.global_tokens_per_window
        )

        if customer_budget_exhausted and not hot_lane_can_borrow:
            return self._defer(
                customer_id=customer_id,
                campaign_id=campaign_id,
                interaction_id=interaction_id,
                estimated_tokens=tokens,
                retry_after_seconds=retry_after,
                reason="customer_token_budget_exhausted",
                lane=normalized_lane,
            )

        await self._increment_window_counter(keys["global_requests"], 1)
        await self._increment_window_counter(keys["global_tokens"], tokens)
        await self._increment_window_counter(keys["customer_tokens"], tokens)

        reason = "allowed"
        if customer_budget_exhausted and hot_lane_can_borrow:
            reason = "allowed_hot_lane_borrowed_global_headroom"

        decision = LLMScheduleDecision(
            allowed=True,
            retry_after_seconds=0,
            reason=reason,
            customer_id=customer_id,
            campaign_id=campaign_id,
            interaction_id=interaction_id,
            estimated_tokens=tokens,
        )
        logger.info(
            "llm_budget_reserved",
            extra={
                "customer_id": customer_id,
                "campaign_id": campaign_id,
                "interaction_id": interaction_id,
                "lane": normalized_lane,
                "estimated_tokens": tokens,
                "reason": reason,
                "global_requests_used": global_requests_used + 1,
                "global_request_limit": self.global_requests_per_window,
                "global_tokens_used": global_tokens_used + tokens,
                "global_token_limit": self.global_tokens_per_window,
                "customer_tokens_used": customer_tokens_used + tokens,
                "customer_token_limit": customer_limit,
            },
        )
        return decision

    async def record_actual_usage(
        self,
        *,
        customer_id: str,
        campaign_id: str,
        interaction_id: str,
        actual_tokens: int,
        lane: str = COLD_LANE,
        estimated_tokens: Optional[int] = None,
    ) -> LLMUsageRecord:
        """
        Record provider-reported token usage after an allowed LLM call returns.

        Reservation counters are intentionally not decremented when actual usage
        is lower than the estimate. That conservative behavior prevents many
        concurrent calls from overbooking the provider limit within the same
        fixed window.
        """
        tokens = max(0, int(actual_tokens))
        estimated = max(1, int(estimated_tokens or self.avg_tokens_per_call))
        normalized_lane = lane.lower().strip()
        window_id = self._current_window_id()
        keys = self._keys(window_id, customer_id)

        await self._increment_window_counter(keys["global_actual_tokens"], tokens)
        await self._increment_window_counter(keys["customer_actual_tokens"], tokens)

        usage = LLMUsageRecord(
            customer_id=customer_id,
            campaign_id=campaign_id,
            interaction_id=interaction_id,
            lane=normalized_lane,
            estimated_tokens=estimated,
            actual_tokens=tokens,
        )
        logger.info(
            "llm_usage_recorded",
            extra={
                "customer_id": customer_id,
                "campaign_id": campaign_id,
                "interaction_id": interaction_id,
                "lane": normalized_lane,
                "estimated_tokens": estimated,
                "actual_tokens": tokens,
            },
        )
        return usage

    async def _read_int(self, key: str) -> int:
        value = await self.backend.get(key)
        return int(value or 0)

    async def _increment_window_counter(self, key: str, amount: int) -> int:
        value = await self.backend.incrby(key, int(amount))
        await self.backend.expire(key, self.window_seconds + 5)
        return int(value)

    async def _retry_after_seconds(self, key: str) -> int:
        ttl = await self.backend.ttl(key)
        if ttl is None or ttl < 0:
            return self.window_seconds
        return max(1, int(ttl))

    def _defer(
        self,
        *,
        customer_id: str,
        campaign_id: str,
        interaction_id: str,
        estimated_tokens: int,
        retry_after_seconds: int,
        reason: str,
        lane: str,
    ) -> LLMScheduleDecision:
        decision = LLMScheduleDecision(
            allowed=False,
            retry_after_seconds=retry_after_seconds,
            reason=reason,
            customer_id=customer_id,
            campaign_id=campaign_id,
            interaction_id=interaction_id,
            estimated_tokens=estimated_tokens,
        )
        self._log_deferred(decision, lane=lane)
        return decision

    def _log_deferred(self, decision: LLMScheduleDecision, *, lane: str) -> None:
        logger.info(
            "llm_budget_deferred",
            extra={
                "customer_id": decision.customer_id,
                "campaign_id": decision.campaign_id,
                "interaction_id": decision.interaction_id,
                "lane": lane,
                "estimated_tokens": decision.estimated_tokens,
                "retry_after_seconds": decision.retry_after_seconds,
                "reason": decision.reason,
            },
        )

    def _current_window_id(self) -> int:
        return int(time.time() // self.window_seconds)

    def _keys(self, window_id: int, customer_id: str) -> dict[str, str]:
        prefix = f"llm:rate_limit:{window_id}"
        return {
            "global_requests": f"{prefix}:global:requests",
            "global_tokens": f"{prefix}:global:reserved_tokens",
            "global_actual_tokens": f"{prefix}:global:actual_tokens",
            "customer_tokens": f"{prefix}:customer:{customer_id}:reserved_tokens",
            "customer_actual_tokens": f"{prefix}:customer:{customer_id}:actual_tokens",
        }


llm_scheduler = LLMScheduler()
