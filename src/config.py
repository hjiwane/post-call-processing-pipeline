import os


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/voicebot"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", "redis://localhost:6379/2"
    )

    # ── LLM ───────────────────────────────────────────────────────────────────
    # One provider, one model, one key. Everyone shares it.
    # These limits come straight from the provider's dashboard — they are HARD
    # limits that result in 429 errors when exceeded, not soft suggestions.
    #
    # At 100K calls/campaign: if even 10% hit the LLM concurrently that's
    # 10,000 requests fighting for 500 slots/min. You do the math.
    #
    # Worth noting: LLM_TOKENS_PER_MINUTE and LLM_REQUESTS_PER_MINUTE are
    # defined here but grep the codebase — nothing actually reads them before
    # firing a request. They exist as documentation, not enforcement.
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "sk-mock-key-for-assessment")
    LLM_TOKENS_PER_MINUTE: int = int(os.getenv("LLM_TOKENS_PER_MINUTE", "90000"))
    LLM_REQUESTS_PER_MINUTE: int = int(os.getenv("LLM_REQUESTS_PER_MINUTE", "500"))

    # Fixed-window scheduling knobs used before any LLM request is made.
    # Customer budget is intentionally lower than the global provider budget so
    # one customer cannot consume the entire shared provider quota.
    CUSTOMER_DEFAULT_TOKENS_PER_MINUTE: int = int(
        os.getenv("CUSTOMER_DEFAULT_TOKENS_PER_MINUTE", "15000")
    )
    LLM_HOT_LANE_BORROW_ENABLED: bool = (
        os.getenv("LLM_HOT_LANE_BORROW_ENABLED", "true").lower() == "true"
    )
    LLM_RATE_LIMIT_WINDOW_SECONDS: int = int(
        os.getenv("LLM_RATE_LIMIT_WINDOW_SECONDS", "60")
    )

    # Average tokens consumed per post-call analysis (measured from prod logs).
    # Useful if you're trying to estimate how many calls can be processed per
    # minute before hitting LLM_TOKENS_PER_MINUTE.
    LLM_AVG_TOKENS_PER_CALL: int = int(os.getenv("LLM_AVG_TOKENS_PER_CALL", "1500"))

    # ── Recording ─────────────────────────────────────────────────────────────
    # Recording availability is eventually consistent. Poll with bounded retry
    # instead of blocking a worker for one hardcoded sleep and then giving up.
    RECORDING_INITIAL_DELAY_SECONDS: int = int(
        os.getenv("RECORDING_INITIAL_DELAY_SECONDS", "5")
    )
    RECORDING_MAX_ATTEMPTS: int = int(os.getenv("RECORDING_MAX_ATTEMPTS", "6"))
    RECORDING_BACKOFF_SECONDS: int = int(os.getenv("RECORDING_BACKOFF_SECONDS", "10"))
    S3_BUCKET: str = os.getenv("S3_BUCKET", "voicebot-recordings")

    # ── Circuit breaker ───────────────────────────────────────────────────────
    # When LLM usage hits 90% of capacity, the circuit breaker trips and the
    # dialler freezes for 30 minutes. This was meant to prevent 429s.
    # In practice it just means the dialler stops making calls while the LLM
    # queue drains — business impact: zero new calls for half an hour.
    #
    # 1800 seconds = 30 minutes. The sales team noticed before the engineers did.
    CIRCUIT_BREAKER_CAPACITY_THRESHOLD: float = 0.90
    CIRCUIT_BREAKER_FREEZE_SECONDS: int = 1800

    # ── Post-call processing ──────────────────────────────────────────────────
    # Single queue. Everything goes here. A "not interested" 10-second call
    # and a confirmed rebook sit in the same line at the same priority.
    POSTCALL_CELERY_QUEUE: str = "postcall_processing"
    POSTCALL_MAX_RETRIES: int = 3
    POSTCALL_RETRY_DELAY: int = 60  # Fixed delay — not exponential backoff


settings = Settings()
