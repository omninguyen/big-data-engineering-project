"""One ingest batch from start to manifest: source -> bronze data files + manifest (ingest/__init__.py)."""
import logging

from common import logs
from common.errors import MissingCredentials
from ingest import bronze, sources
from ingest.batch import FAILED, PARTIAL, SKIPPED, SUCCESS, Batch

log = logging.getLogger(__name__)


def run(source_name: str, logical_date: str) -> dict:
    """Returns the final manifest. A failed batch raises, after its FAILED manifest is written, so the Databricks
    task fails and is retried."""
    source = sources.get(source_name)
    batch = Batch(source_name, logical_date, params=source.params())
    with logs.batch_context(batch.batch_id):
        path = bronze.write_manifest(batch)  # RUNNING
        log.info("%s batch started (dt=%s), manifest %s", source_name, logical_date, path)
        writer = bronze.BatchWriter(batch)
        try:
            for page in source.extract(batch):
                writer.write(page)
            writer.close()
        except MissingCredentials as e:
            batch.finish(SKIPPED, e)
            log.warning("%s batch SKIPPED: %s", source_name, e)
        except Exception as e:
            batch.finish(FAILED, e)
            try:
                bronze.write_manifest(batch)
            except Exception:
                log.exception("could not write the FAILED manifest")
            log.error("%s", _summary(batch))
            raise
        else:
            batch.finish(PARTIAL if batch.pages_failed else SUCCESS)
        bronze.write_manifest(batch)
        (log.warning if batch.status == PARTIAL else log.info)("%s", _summary(batch))
    return batch.manifest()


def _summary(batch: Batch) -> str:
    entities = ", ".join(f"{name} {e['records']}" for name, e in batch.entities.items()) or "none"
    return (f"{batch.source} batch {batch.status}: {batch.records} records ({entities}), "
            f"{batch.pages_failed} failed pages, {batch.http.requests} HTTP requests ({batch.http.retries} retries)")
