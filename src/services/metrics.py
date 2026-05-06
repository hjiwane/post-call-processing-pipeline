"""
PostCallMetricsTracker — Records timing and outcome metrics for post-call processing.

Currently: logs to stdout. That's it.

What we can't answer from these logs without significant grep work:
  - What's the p95 latency for post-call analysis over the last hour?
  - How many tokens did Customer X consume today?
  - What percentage of calls are failing at the LLM step vs the recording step?
  - How deep is the processing backlog right now?
  - Are we trending toward hitting the LLM rate limit in the next 10 minutes?

The data to answer these questions exists — it flows through this class with
every interaction. It's just not being captured in a form that's queryable.

tokens_used is particularly interesting: we get the exact value from the LLM
provider on every call and log it. If we also wrote it to Redis with a sliding
window, we'd have real-time TPM visibility. If we bucketed it by customer_id,
we'd have per-customer usage tracking. Neither of those things requires a big
infrastructure change — just a few more Redis writes in track_processing_completed.
"""

import logging
import time

from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


class PostCallMetricsTracker:

    async def track_stage(
        self,
        interaction_id: str,
        stage: str,
        *,
        customer_id: str | None = None,
        campaign_id: str | None = None,
        lane: str | None = None,
        status: str = "ok",
        **extra,
    ) -> None:
        """Emit lightweight structured metrics for pipeline stage transitions."""
        logger.info(
            "postcall_stage_metric",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "campaign_id": campaign_id,
                "lane": lane,
                "stage": stage,
                "status": status,
                **extra,
            },
        )

    async def track_processing_started(self, interaction_id: str) -> None:
        """Record the wall-clock start time for an interaction's processing."""
        try:
            await redis_client.set(
                f"postcall:metrics:{interaction_id}:start",
                str(time.time()),
                ex=3600,  # 1-hour TTL — if processing takes longer than that, we lose the start time
            )
        except Exception as exc:
            logger.warning(
                "postcall_metrics_start_failed",
                extra={"interaction_id": interaction_id, "error": str(exc)},
            )

    async def track_processing_completed(
        self,
        interaction_id: str,
        tokens_used: int,
        latency_ms: float,
        *,
        customer_id: str | None = None,
        campaign_id: str | None = None,
        lane: str | None = None,
    ) -> None:
        """
        Log completion metrics for a processed interaction.

        tokens_used is the exact value from the LLM provider's response.
        It's logged here but not written to any aggregate counter.

        If you wanted to know "how many tokens did we use in the last 60 seconds?"
        you'd need to INCRBY a Redis key here, with a 60-second TTL, and do it
        per-customer if you want customer-level visibility.
        """
        try:
            start = await redis_client.get(f"postcall:metrics:{interaction_id}:start")
        except Exception as exc:
            logger.warning(
                "postcall_metrics_read_failed",
                extra={"interaction_id": interaction_id, "error": str(exc)},
            )
            start = None
        wall_time_s = time.time() - float(start) if start else 0

        logger.info(
            "postcall_metrics",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "campaign_id": campaign_id,
                "lane": lane,
                "tokens_used": tokens_used,
                "llm_latency_ms": latency_ms,
                "total_wall_time_s": round(wall_time_s, 2),
                # wall_time_s includes the 45s recording sleep + LLM latency.
                # If wall_time_s >> llm_latency_ms, recording is the bottleneck.
                # That ratio could be a useful signal — but only if someone
                # is looking at it, which currently nobody is.
            },
        )

    async def track_processing_failed(
        self, interaction_id: str, error: str
    ) -> None:
        """
        Log a processing failure.

        This is called by the retry queue when max retries are exhausted.
        After this log line, the interaction has no further processing scheduled.
        It will stay in "pending" state in the dashboard indefinitely.
        """
        logger.error(
            "postcall_failed_permanently",
            extra={
                "interaction_id": interaction_id,
                "error": error,
                # A Grafana alert on postcall_failed_permanently would catch
                # interactions that fell through completely. Currently none exists.
            },
        )


metrics_tracker = PostCallMetricsTracker()
