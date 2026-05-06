# Post-Call Processing Pipeline

This project implements a rate-limit-aware post-call processing pipeline for a voice AI platform. The goal is to safely process large volumes of completed calls without overwhelming external LLM providers, losing jobs, or creating uncontrolled retry backlogs.

The main improvement is that the system now decides whether an LLM call is allowed before calling the provider. It uses global LLM limits, per-customer token budgets, cheap transcript triage, recording polling, and structured audit logs to make the pipeline safer and easier to operate.

---

## Problem

The original pipeline triggered post-call LLM analysis directly after a call completed. This works at small scale, but fails when volume increases.

At high traffic, for example 100K completed calls, the system can hit:

- LLM request-per-minute limits
- LLM token-per-minute limits
- customer-specific quota issues
- repeated 429 errors
- Celery retry storms
- Redis backlog growth
- silent job failures
- delayed recordings not being available yet

The fix is to schedule and budget LLM work before making the expensive call.

---

## Solution Overview

The updated pipeline introduces four main improvements:

1. **Cheap triage before LLM analysis**
   - Calls are classified into `hot`, `cold`, or `skip`
   - Short calls and wrong-number calls skip LLM processing
   - High-value calls such as rebookings, demos, or escalations are prioritized

2. **Rate-limit-aware LLM scheduling**
   - Global request-per-minute limit
   - Global token-per-minute limit
   - Per-customer token budgets
   - No LLM call happens unless quota is available

3. **Recording polling**
   - Replaces fixed waiting with retry and backoff
   - Handles delayed recording availability
   - Failing recordings are logged visibly and do not block LLM analysis

4. **Auditability**
   - Major stages emit structured audit events
   - Logs include `interaction_id`
   - Failures are easier to trace and debug

---

## Architecture

```text
Call Completed
     |
     v
FastAPI Endpoint
     |
     v
Load interaction + transcript
     |
     v
Cheap Triage
 hot / cold / skip
     |
     v
Celery Post-Call Task
     |
     +---------------------------+
     |                           |
     v                           v
Recording Poller             LLM Scheduler
 retry/backoff               global + customer budget
     |                           |
     v                           v
S3 Upload                  LLM Analysis
     |                           |
     +-------------+-------------+
                   |
                   v
        Signal Jobs + Lead Stage Update
                   |
                   v
             Audit + Metrics
