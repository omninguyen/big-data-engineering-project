"""Logging: every log record carries the id of the batch being ingested ("-" outside a batch), so the output of a
Databricks task can be searched by batch_id."""
import contextlib
import contextvars
import logging

FORMAT = "%(asctime)s %(levelname)s %(name)s [%(batch_id)s]: %(message)s"
_BATCH_ID = contextvars.ContextVar("batch_id", default="-")


def install() -> None:
    """Add the attribute batch_id to every log record (idempotent)."""
    factory = logging.getLogRecordFactory()
    if getattr(factory, "with_batch_id", False):
        return

    def with_batch_id(*args, **kwargs):
        record = factory(*args, **kwargs)
        record.batch_id = _BATCH_ID.get()
        return record

    with_batch_id.with_batch_id = True
    logging.setLogRecordFactory(with_batch_id)


def setup(level: int = logging.INFO) -> None:
    install()
    logging.basicConfig(level=level, format=FORMAT)


@contextlib.contextmanager
def batch_context(batch_id: str):
    token = _BATCH_ID.set(batch_id)
    try:
        yield
    finally:
        _BATCH_ID.reset(token)
