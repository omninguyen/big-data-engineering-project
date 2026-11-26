"""Shared exceptions for the ingest steps.

Each type tells the batch runner (src/ingest/runner.py) how to react:
    MissingCredentials  optional source has no credentials configured -> batch SKIPPED, not an error
    SourceError         source failed or returned unusable data -> batch FAILED, the task fails and is retried

Messages of every PipelineError subclass are redacted (api_key=..., Bearer ...). Exceptions raised by
libraries (requests) are not: when wrapping one, use `raise ... from None` so the traceback does
not print the original error, which may contain the full URL.
"""
import re

__all__ = ["redact", "PipelineError", "MissingCredentials", "SourceError"]

_SECRET_PARAM = re.compile(r"(?i)\b(api_key|apikey|access_token|token|password|secret)=[^&\s'\"]+")
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")


def redact(text) -> str:
    """Mask secret values in query strings and Authorization headers."""
    text = _SECRET_PARAM.sub(lambda m: f"{m.group(1)}=***", str(text))
    return _BEARER.sub("Bearer ***", text)


class PipelineError(Exception):
    """Base class for errors raised on purpose by the pipeline."""

    def __init__(self, message: str):
        super().__init__(redact(message))


class MissingCredentials(PipelineError):
    """An optional source has no credentials; the job runner turns this into a skip."""


class SourceError(PipelineError):
    """A data source (API, file) failed or returned unusable data.

    source: source name, e.g. "tmdb"; status: HTTP status code, if any.
    """

    def __init__(self, source: str, message: str, status: int | None = None):
        self.source = source
        self.status = status
        self._message = message
        super().__init__(f"[{source}] {message}")

    def __reduce__(self):  # keep source/status when the exception is pickled (multiprocessing, Spark)
        return type(self), (self.source, self._message, self.status)
