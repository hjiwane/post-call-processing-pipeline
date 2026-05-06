"""
PostCallProcessor — Runs LLM analysis on a completed call transcript.

This is where the LLM quota gets spent. Every call that reaches this class
consumes ~1,500 tokens on average (see settings.LLM_AVG_TOKENS_PER_CALL).

The prompt extracts three things in a single LLM call (single_prompt=True):
  - call_stage: the outcome/disposition ("rebook_confirmed", "not_interested", etc.)
  - entities: structured data mentioned in the call (dates, amounts, names)
  - summary: a human-readable summary for the dashboard

This implementation now gates the expensive LLM call behind cheap deterministic
triage plus a rate-limit-aware scheduler. Short/skip calls never reserve quota
and never call the LLM. Hot/cold calls must reserve budget before _call_llm is
called, and actual usage is recorded after the mocked provider response returns.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass

from src.config import settings
from src.services.llm_scheduler import LLMScheduler, LLMScheduleDecision, llm_scheduler
from src.services.triage import ProcessingLane, TranscriptTriage, transcript_triage

logger = logging.getLogger(__name__)


@dataclass
class PostCallContext:
    """Everything needed to process one completed call."""
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str  # The business using the platform (not the person called)
    agent_id: str
    call_sid: str     # Exotel's identifier for the call
    transcript_text: str
    conversation_data: dict
    additional_data: dict  # Arbitrary metadata from the dialler (campaign config, etc.)
    ended_at: datetime
    exotel_account_id: Optional[str] = None


@dataclass
class AnalysisResult:
    call_stage: str          # Disposition: rebook_confirmed, not_interested, etc.
    entities: Dict[str, Any] # Structured entities extracted from the transcript
    summary: str             # Human-readable summary for dashboard display
    raw_response: Dict[str, Any]
    tokens_used: int         # Actual tokens consumed — source of truth for billing
    latency_ms: float
    provider: str
    model: str


class RateLimitDeferred(Exception):
    """Raised when LLM capacity is unavailable and the job should retry later."""

    def __init__(self, decision: LLMScheduleDecision):
        self.retry_after_seconds = decision.retry_after_seconds
        self.reason = decision.reason
        self.customer_id = decision.customer_id
        self.campaign_id = decision.campaign_id
        self.interaction_id = decision.interaction_id
        self.estimated_tokens = decision.estimated_tokens
        super().__init__(
            f"LLM analysis deferred for {decision.retry_after_seconds}s: {decision.reason}"
        )


class PostCallProcessor:
    """
    Runs full LLM analysis on a transcript.

    The processor now applies two cheap gates before spending LLM quota:
      1. deterministic transcript triage, where skip/short calls return a
         zero-token AnalysisResult;
      2. scheduler reservation for hot/cold calls, so _call_llm is never called
         without available global and customer capacity.
    """

    def __init__(
        self,
        *,
        scheduler: Optional[LLMScheduler] = None,
        triage: Optional[TranscriptTriage] = None,
    ) -> None:
        self.scheduler = scheduler or llm_scheduler
        self.triage = triage or transcript_triage

    async def process_post_call(
        self, ctx: PostCallContext, single_prompt: bool = True
    ) -> AnalysisResult:
        """
        Run LLM analysis and write result to interaction_metadata.

        Short/skip calls are handled locally without scheduler or LLM usage.
        Hot/cold calls must successfully reserve quota before _call_llm runs.
        """
        triage_result = self.triage.triage(ctx.conversation_data or ctx.transcript_text)
        lane = triage_result.lane.value

        if triage_result.lane == ProcessingLane.SKIP:
            result = self._build_skipped_result(triage_result.detected_stage)
            await self._update_interaction_metadata(ctx.interaction_id, result)
            logger.info(
                "postcall_analysis_skipped",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "lane": lane,
                    "call_stage": result.call_stage,
                    "reason": triage_result.reason,
                    "estimated_tokens": 0,
                    "actual_tokens": 0,
                },
            )
            return result

        prompt = self._build_analysis_prompt(
            ctx.transcript_text,
            ctx.additional_data,
            single_prompt,
        )
        estimated_tokens = self._estimate_tokens(prompt)

        decision = await self.scheduler.reserve_budget(
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            interaction_id=ctx.interaction_id,
            lane=lane,
            estimated_tokens=estimated_tokens,
        )

        if not decision.allowed:
            logger.info(
                "postcall_analysis_deferred",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "lane": lane,
                    "estimated_tokens": decision.estimated_tokens,
                    "retry_after_seconds": decision.retry_after_seconds,
                    "reason": decision.reason,
                },
            )
            raise RateLimitDeferred(decision)

        try:
            logger.info(
                "postcall_analysis_llm_call_started",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "lane": lane,
                    "estimated_tokens": estimated_tokens,
                },
            )

            start_time = datetime.utcnow()
            response = await self._call_llm(prompt)
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

            result = self._parse_response(response, elapsed_ms)
            await self.scheduler.record_actual_usage(
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                interaction_id=ctx.interaction_id,
                lane=lane,
                estimated_tokens=estimated_tokens,
                actual_tokens=result.tokens_used,
            )

            # Result written to interaction_metadata — the dashboard's hot cache.
            await self._update_interaction_metadata(ctx.interaction_id, result)

            logger.info(
                "postcall_analysis_complete",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "lane": lane,
                    "call_stage": result.call_stage,
                    "estimated_tokens": estimated_tokens,
                    "actual_tokens": result.tokens_used,
                    "tokens_used": result.tokens_used,
                    "latency_ms": result.latency_ms,
                },
            )

            return result

        except Exception as e:
            logger.exception(
                "postcall_analysis_failed",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "lane": lane,
                    "estimated_tokens": estimated_tokens,
                    "error": str(e),
                },
            )
            raise

    def _build_skipped_result(self, detected_stage: str) -> AnalysisResult:
        response = {
            "call_stage": detected_stage,
            "entities": {},
            "summary": "Skipped full LLM analysis by deterministic transcript triage.",
            "usage": {"total_tokens": 0},
        }
        return AnalysisResult(
            call_stage=detected_stage,
            entities={},
            summary=response["summary"],
            raw_response=response,
            tokens_used=0,
            latency_ms=0.0,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    def _estimate_tokens(self, prompt: str) -> int:
        """
        Cheap pre-call estimate used for quota reservation.

        Keep the assessment implementation simple and conservative: use the
        configured average as the floor, with a rough character-based estimate
        for unusually long prompts.
        """
        rough_prompt_tokens = max(1, len(prompt) // 4)
        return max(settings.LLM_AVG_TOKENS_PER_CALL, rough_prompt_tokens)

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        single_prompt: bool,
    ) -> str:
        """
        Build the LLM prompt.

        The system prompt asks for three outputs in one JSON object.
        call_stage is the most important — everything downstream depends on it.
        entities and summary are useful but secondary.
        """
        system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call

Respond in JSON format:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        return (
            f"{system_prompt}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )

    async def _call_llm(self, prompt: str) -> dict:
        """
        Call the configured LLM provider.

        Mock implementation for the assessment. No real provider call happens.
        """
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result",
            "usage": {"total_tokens": 1500},
        }

    def _parse_response(self, response: dict, latency_ms: float) -> AnalysisResult:
        return AnalysisResult(
            call_stage=response.get("call_stage", "unknown"),
            entities=response.get("entities", {}),
            summary=response.get("summary", ""),
            raw_response=response,
            tokens_used=response.get("usage", {}).get("total_tokens", 0),
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    async def _update_interaction_metadata(
        self, interaction_id: str, result: AnalysisResult
    ) -> None:
        """
        Write analysis results into the interaction_metadata JSONB column.
        """
        logger.info(
            "metadata_updated",
            extra={
                "interaction_id": interaction_id,
                "call_stage": result.call_stage,
                "actual_tokens": result.tokens_used,
            },
        )


post_call_processor = PostCallProcessor()
