try:
    from celery import Celery
except ModuleNotFoundError:  # pragma: no cover - local test fallback
    Celery = None

from src.config import settings


class _FakeAsyncResult:
    id = "local-test-task-id"


class _FakeCeleryTask:
    def __init__(self, fn):
        self.fn = fn
        self.__name__ = getattr(fn, "__name__", "fake_celery_task")
        self.__doc__ = getattr(fn, "__doc__", None)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def apply_async(self, *args, **kwargs):
        return _FakeAsyncResult()

    def retry(self, exc=None, countdown=None):
        if exc:
            raise exc
        raise RuntimeError("fake celery retry requested")


class _FakeCelery:
    def __init__(self, *args, **kwargs):
        self.conf = {}

    def task(self, *decorator_args, **decorator_kwargs):
        def decorate(fn):
            return _FakeCeleryTask(fn)
        return decorate


if Celery is not None:
    celery_app = Celery(
        "voicebot",
        broker=settings.CELERY_BROKER_URL,
        backend=settings.CELERY_RESULT_BACKEND,
    )
    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_default_queue=settings.POSTCALL_CELERY_QUEUE,
    )
else:
    celery_app = _FakeCelery()
