"""
Cheap deterministic transcript triage for post-call processing.

The goal is to avoid spending LLM quota on calls that are clearly low-value or
not actionable. This is intentionally rule-based: it is fast, explainable, and
safe to run before the expensive LLM analysis step.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping


class ProcessingLane(str, Enum):
    HOT = "hot"
    COLD = "cold"
    SKIP = "skip"


@dataclass(frozen=True)
class TriageResult:
    lane: ProcessingLane
    reason: str
    detected_stage: str
    confidence: float


class TranscriptTriage:
    """Rule-based pre-screen for deciding whether a call needs full LLM analysis."""

    SHORT_TRANSCRIPT_TURN_LIMIT = 4

    # Ordered from most decisive/actionable to least. The first matching rule
    # wins, so escalation and bookings take precedence over generic callbacks.
    HOT_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "escalation_needed",
            (
                "speak to a manager",
                "file a complaint",
                "this is unacceptable",
                "nobody helps",
                "escalate this",
                "escalate",
                "complaint",
                "senior executive",
            ),
        ),
        (
            "demo_booked",
            (
                "demo is booked",
                "live demo",
                "calendar invite",
                "meeting link",
                "block my calendar",
                "see you then",
                "demo booked",
            ),
        ),
        (
            "rebook_confirmed",
            (
                "rebook",
                "reschedule",
                "tomorrow works",
                "i'll book your slot",
                "book your slot",
                "confirmed",
                "theek hai, confirmed",
            ),
        ),
    )

    COLD_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "not_interested",
            (
                "not interested",
                "don't call me again",
                "do not call me again",
                "please don't call",
                "stop calling",
            ),
        ),
        (
            "already_done",
            (
                "already booked",
                "already purchased",
                "already bought",
                "already completed",
                "already done",
                "through your website",
            ),
        ),
        (
            "callback_requested",
            (
                "call me back",
                "call you back",
                "call karo",
                "baad mein call",
                "after 6 pm",
                "meeting mein hoon",
                "later",
            ),
        ),
        (
            "considering",
            (
                "thinking",
                "considering",
                "soch raha",
                "sochna padega",
                "dekhta hoon",
                "budget tight",
                "next week",
            ),
        ),
    )

    SKIP_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "wrong_number",
            (
                "wrong number",
                "galat number",
                "not my number",
                "you have the wrong",
            ),
        ),
    )

    def triage(self, transcript: Any) -> TriageResult:
        turns = self._extract_turns(transcript)
        text = self._normalize_text(turns)

        if len(turns) < self.SHORT_TRANSCRIPT_TURN_LIMIT:
            detected_stage = "short_call"
            if self._matches_any(text, self.SKIP_RULES):
                detected_stage = "wrong_number"
            return TriageResult(
                lane=ProcessingLane.SKIP,
                reason="transcript_has_fewer_than_4_turns",
                detected_stage=detected_stage,
                confidence=0.98,
            )

        matched_skip = self._match_stage(text, self.SKIP_RULES)
        if matched_skip:
            return TriageResult(
                lane=ProcessingLane.SKIP,
                reason=f"matched_skip_rule:{matched_skip}",
                detected_stage=matched_skip,
                confidence=0.95,
            )

        matched_hot = self._match_stage(text, self.HOT_RULES)
        if matched_hot:
            return TriageResult(
                lane=ProcessingLane.HOT,
                reason=f"matched_hot_rule:{matched_hot}",
                detected_stage=matched_hot,
                confidence=0.9,
            )

        matched_cold = self._match_stage(text, self.COLD_RULES)
        if matched_cold:
            return TriageResult(
                lane=ProcessingLane.COLD,
                reason=f"matched_cold_rule:{matched_cold}",
                detected_stage=matched_cold,
                confidence=0.85,
            )

        return TriageResult(
            lane=ProcessingLane.COLD,
            reason="no_decisive_hot_or_skip_rule_matched",
            detected_stage="ambiguous",
            confidence=0.55,
        )

    def _extract_turns(self, transcript: Any) -> list[str]:
        if transcript is None:
            return []

        if isinstance(transcript, str):
            return [line.strip() for line in transcript.splitlines() if line.strip()]

        if isinstance(transcript, Mapping):
            nested = transcript.get("transcript") or transcript.get("turns") or []
            return self._extract_turns(nested)

        if isinstance(transcript, Iterable):
            turns: list[str] = []
            for turn in transcript:
                if isinstance(turn, Mapping):
                    role = str(turn.get("role", "")).strip()
                    content = str(turn.get("content", "")).strip()
                    if content:
                        turns.append(f"{role}: {content}" if role else content)
                elif turn is not None:
                    content = str(turn).strip()
                    if content:
                        turns.append(content)
            return turns

        return [str(transcript).strip()] if str(transcript).strip() else []

    def _normalize_text(self, turns: list[str]) -> str:
        return "\n".join(turns).lower()

    def _match_stage(
        self, text: str, rules: tuple[tuple[str, tuple[str, ...]], ...]
    ) -> str | None:
        for stage, keywords in rules:
            if any(keyword in text for keyword in keywords):
                return stage
        return None

    def _matches_any(
        self, text: str, rules: tuple[tuple[str, tuple[str, ...]], ...]
    ) -> bool:
        return self._match_stage(text, rules) is not None


transcript_triage = TranscriptTriage()
