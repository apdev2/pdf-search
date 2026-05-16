import argparse
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

import config
from es_setup import create_indices, set_refresh_interval

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def index_pages(es_client: Elasticsearch, pages_iterable) -> tuple[int, int]:
    now = datetime.now(timezone.utc).isoformat()

    def _actions():
        for rec in pages_iterable:
            yield {
                "_index": config.INDEX_NAME,
                "_id": f"{rec['pdf_id']}_page_{rec['page_number']}",
                "_source": {
                    "pdf_id": rec["pdf_id"],
                    "page_number": rec["page_number"],
                    "text": rec["text"],
                    "pdf_filename": rec["pdf_filename"],
                    "indexed_at": now,
                },
            }

    indexed = 0
    failed = 0
    for ok, item in parallel_bulk(
        es_client,
        _actions(),
        chunk_size=config.BULK_CHUNK_SIZE,
        thread_count=config.BULK_THREAD_COUNT,
        raise_on_error=False,
    ):
        if ok:
            indexed += 1
        else:
            failed += 1
            log.warning("Indexing failure: %s", item)
        if (indexed + failed) % 10_000 == 0:
            log.info("Progress: %d indexed, %d failed", indexed, failed)

    es_client.indices.refresh(index=config.INDEX_NAME)
    set_refresh_interval(es_client, config.INDEX_NAME, config.REFRESH_INTERVAL_SERVING)
    log.info("Indexing complete: %d indexed, %d failed", indexed, failed)
    return indexed, failed


def upsert_pdf_metadata(
    es_client: Elasticsearch,
    pdf_id: str,
    pdf_filename: str,
    total_pages: int,
    file_size_bytes: int | None = None,
    tags: list[str] | None = None,
    description: str | None = None,
    extra_metadata: dict | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "pdf_id": pdf_id,
        "pdf_filename": pdf_filename,
        "total_pages": total_pages,
        "last_indexed_at": now,
    }
    if file_size_bytes is not None:
        doc["file_size_bytes"] = file_size_bytes
    if tags is not None:
        doc["tags"] = tags
    if description is not None:
        doc["description"] = description
    if extra_metadata is not None:
        doc["extra_metadata"] = extra_metadata

    es_client.update(
        index=config.METADATA_INDEX_NAME,
        id=pdf_id,
        body={
            "scripted_upsert": True,
            "script": {
                "source": "ctx._source.putAll(params.doc); if (ctx.op == 'create') { ctx._source.uploaded_at = params.now; }",
                "params": {"doc": doc, "now": now},
            },
            "upsert": {},
        },
    )
    log.info("Upserted metadata for %s", pdf_id)


def bulk_upsert_metadata(es_client: Elasticsearch, metadata_list: list[dict]) -> None:
    for meta in metadata_list:
        upsert_pdf_metadata(
            es_client,
            pdf_id=meta["pdf_id"],
            pdf_filename=meta["pdf_filename"],
            total_pages=meta["total_pages"],
            file_size_bytes=meta.get("file_size_bytes"),
            tags=meta.get("tags"),
            description=meta.get("description"),
            extra_metadata=meta.get("extra_metadata"),
        )


def read_pages_from_directory(base_path: str):
    base = Path(base_path)
    for pdf_dir in sorted(base.iterdir()):
        if not pdf_dir.is_dir():
            continue
        pdf_id = pdf_dir.name
        pdf_filename = pdf_id + ".pdf"
        for page_file in sorted(pdf_dir.iterdir()):
            if not page_file.is_file() or not page_file.suffix == ".txt":
                continue
            match = re.match(r"page_(\d+)\.txt", page_file.name)
            if not match:
                continue
            page_number = int(match.group(1))
            text = page_file.read_text(encoding="utf-8")
            yield {
                "pdf_id": pdf_id,
                "page_number": page_number,
                "text": text,
                "pdf_filename": pdf_filename,
            }


def _generate_fake_pages(count: int):
    from faker import Faker

    fake = Faker()
    pages_per_pdf = 10
    for i in range(count):
        pdf_index = i // pages_per_pdf
        page_number = (i % pages_per_pdf) + 1
        pdf_id = f"pdf_{pdf_index:04d}"
        yield {
            "pdf_id": pdf_id,
            "page_number": page_number,
            "text": fake.paragraph(nb_sentences=10),
            "pdf_filename": f"{pdf_id}.pdf",
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index PDF pages into Elasticsearch")
    parser.add_argument("--source-dir", help="Directory containing extracted PDF text files")
    parser.add_argument("--sample", type=int, help="Generate and index N fake pages")
    args = parser.parse_args()

    es = Elasticsearch(config.ES_HOSTS, retry_on_timeout=True, max_retries=3)
    log.info("Connected to ES: %s", es.info()["version"]["number"])

    if args.source_dir:
        pages = read_pages_from_directory(args.source_dir)
    elif args.sample:
        pages = _generate_fake_pages(args.sample)
    else:
        parser.error("Provide --source-dir or --sample")

    start = time.time()
    indexed, failed = index_pages(es, pages)
    elapsed = time.time() - start
    log.info("%.1f seconds, %.0f docs/sec", elapsed, indexed / max(elapsed, 0.001))
