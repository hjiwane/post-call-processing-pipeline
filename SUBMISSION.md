# Post-Call Processing Pipeline — Design Document

**Author:** [Himanshu Jiwane]
**Date:** [06/05/2026]

---

## 1. Assumptions

I treated this as a post-call processing pipeline where the LLM call is the expensive and rate-limited step. The main assumption is that the system should decide whether an LLM call is allowed before calling the provider, instead of calling first and reacting to 429 errors later.

I also assumed that `interaction_id` is the primary trace key, `customer_id` is required for budget isolation, and the current FastAPI/Celery/Redis-style architecture should stay in place for this assignment. Tests should run locally without real LLM, S3, Exotel, Redis, or API keys.

## 2. Problem diagnosis

The original pipeline fired LLM work without awareness of `LLM_REQUESTS_PER_MINUTE` or `LLM_TOKENS_PER_MINUTE`. At high volume, that creates provider 429s, retry storms, Redis/Celery backlog, and difficult-to-debug losses.

There were two related issues: every transcript was treated as worth full LLM analysis, and recording processing waited on a fixed sleep instead of polling for readiness.

The core decision in this implementation is simple: schedule LLM work based on global rate limits and per-customer budgets before calling the provider.

## 3. Architecture overview

```text
Call ended
  -> FastAPI endpoint loads interaction
  -> transcript_text is built
  -> cheap deterministic triage runs
  -> Celery receives lane + trace metadata
       |\
       | \-> recording poller runs independently with retry/backoff
       |
       \-> post-call analysis path
             -> skip lane returns without quota or LLM
             -> hot/cold lanes ask scheduler for quota
             -> if quota unavailable, Celery retries after retry_after_seconds
             -> if quota available, mock LLM runs
             -> actual usage is recorded
             -> lead stage and signal jobs run
             -> audit logs capture each stage
```

## 4. Rate limit management

`src/services/llm_scheduler.py` implements a simple fixed-window scheduler. It enforces:

- global requests per minute
- global tokens per minute
- estimated tokens per call
- configurable window length

If the budget is unavailable, the scheduler returns an explicit deferred decision with `retry_after_seconds` and the LLM is not called.

The scheduler logs:

- `llm_budget_reserved`
- `llm_budget_deferred`
- `llm_usage_recorded`

It accepts an injected Redis-like backend and includes an in-memory backend, so unit tests do not require real Redis.

## 5. Per-customer token budgeting

The scheduler also enforces per-customer token budgets. This prevents one large customer from consuming all available capacity.

The intended behavior is:

```text
Customer A budget exhausted -> Customer A defers
Customer B still has budget -> Customer B can reserve quota
```

The schema includes `customer_llm_budgets` so customer-specific budgets can later be persisted instead of relying only on defaults.

## 6. Differentiated processing: hot / cold / skip

`src/services/triage.py` adds deterministic transcript triage. It does not use an LLM.

Lanes:

- `hot`: rebook confirmed, demo booked, escalation needed
- `cold`: not interested, already purchased/done, callback requested, considering, ambiguous
- `skip`: wrong number or fewer than 4 transcript turns

Short and skip calls never reserve quota and never call the LLM.

This is intentionally cheap and explainable. Ambiguous cases can later be upgraded with a tiny classifier or business-configurable rules.

## 7. Recording pipeline fix

`src/services/recording.py` no longer uses a fixed 45-second sleep. It now polls with configurable retry/backoff:

- `RECORDING_INITIAL_DELAY_SECONDS`
- `RECORDING_MAX_ATTEMPTS`
- `RECORDING_BACKOFF_SECONDS`

Every unavailable attempt is logged with `interaction_id`, `call_sid`, `attempt`, and `next_delay_seconds`.

Successful upload logs `recording_uploaded` with `interaction_id` and `s3_key`.

Permanent failure logs `recording_failed_permanently` at error level and returns `None`. Recording failure is visible, but it does not block LLM analysis.

## 8. Reliability and durability

This implementation keeps Celery/Redis-style execution for local compatibility.

To avoid duplicate retry paths, rate-limit deferrals use Celery retry with `countdown=retry_after_seconds` instead of manually adding a second Redis retry.

The schema now includes durable tables for jobs, audit events, token usage, and customer budgets. Full production-grade worker resume behavior would require wiring Celery workers to those tables or moving to a stronger workflow system.

Production alternatives I would consider:

- Postgres-backed outbox/jobs
- SQS with DLQ
- Kafka retry topics
- Temporal workflows

## 9. Auditability and observability

`src/services/audit.py` adds a structured audit helper. Current implementation logs consistently and is shaped so it can later persist to `postcall_audit_events`.

Major stages include `interaction_id`:

- `postcall_received`
- `postcall_triaged`
- `recording_poll_started`
- `recording_uploaded`
- `recording_failed_permanently`
- `llm_budget_reserved`
- `llm_budget_deferred`
- `llm_analysis_started`
- `llm_analysis_completed`
- `signal_jobs_completed`
- `signal_jobs_failed`
- `lead_stage_updated`
- `lead_stage_failed`
- `postcall_completed`

This makes it much easier to answer why a specific interaction was skipped, deferred, failed, or completed.

## 10. Data model with SQL summary

`data/schema.sql` was updated additively. Existing tables were not deleted.

Added:

- `postcall_jobs`: job state, retry count, next run time, last error
- `postcall_audit_events`: event trail by interaction
- `llm_usage_ledger`: estimated and actual token usage
- `customer_llm_budgets`: customer-level token budgets and priority configuration

Useful indexes were added on:

- `interaction_id`
- `customer_id`
- `campaign_id`
- `status`
- `created_at`

## 11. Security

No real external API calls or secrets were added.

Tests use mocks and in-memory fakes. No LLM, S3, Exotel, Redis, or API keys are required.

In production, I would also add transcript redaction before logs, encryption for recording references, audit retention policies, and role-based access to processing history.

## 12. API interface decisions

The endpoint now does lightweight work only:

- loads the interaction
- updates status
- builds transcript text
- runs cheap triage
- sends lane and trace metadata to Celery

It does not run full LLM analysis directly. It also no longer fires signal jobs with empty analysis for long calls.

## 13. Trade-offs and alternatives considered

I used fixed-window rate limiting instead of a token bucket because it is easier to understand and test in this repo.

I used deterministic triage instead of an ML classifier because the pre-screen should be cheaper than the LLM call it is trying to avoid.

I kept Celery/Redis compatibility instead of introducing Kafka, Temporal, or SQS because those would be too large for this assignment. They are better production options, but not necessary to show the core fix.

## 14. Known weaknesses

The scheduler uses a fixed window, so capacity resets sharply at the window boundary.

Customer-specific budgets are represented in schema, but the scheduler currently uses a default value unless configured/injected.

Audit events are structured in logs, but full DB persistence is not wired yet.

The durable job schema exists, but full worker-resume behavior would need production infrastructure.

Triage is deterministic and may miss nuanced real-world calls. That is acceptable for a cheap first-pass filter, but it should become configurable over time.

## 15. What I would do with more time

I would persist audit events into Postgres, make `postcall_jobs` the source of truth for worker state, and add a small worker loop for deferred jobs.

I would load customer budgets from `customer_llm_budgets`, support burst tokens, and move the fixed-window scheduler to a smoother token-bucket model.

I would also add a dead-letter queue, admin/debug endpoint for interaction history, and campaign-configurable triage rules.

The main design would stay the same: decide whether LLM work is allowed before calling the provider.
