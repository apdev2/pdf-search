import argparse
import logging

from elasticsearch import Elasticsearch

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PDF_PAGES_SETTINGS = {
    "number_of_shards": config.SHARDS,
    "number_of_replicas": config.REPLICAS,
    "refresh_interval": config.REFRESH_INTERVAL_INDEXING,
    "codec": "best_compression",
}

PDF_PAGES_MAPPINGS = {
    "properties": {
        "pdf_id": {"type": "keyword"},
        "page_number": {"type": "integer"},
        "text": {"type": "text", "analyzer": "standard"},
        "pdf_filename": {"type": "keyword"},
        "indexed_at": {"type": "date"},
    }
}

PDF_METADATA_SETTINGS = {
    "number_of_shards": config.METADATA_SHARDS,
    "number_of_replicas": config.REPLICAS,
    "codec": "best_compression",
}

PDF_METADATA_MAPPINGS = {
    "properties": {
        "pdf_id": {"type": "keyword"},
        "pdf_filename": {"type": "keyword"},
        "total_pages": {"type": "integer"},
        "file_size_bytes": {"type": "long"},
        "uploaded_at": {"type": "date"},
        "last_indexed_at": {"type": "date"},
        "tags": {"type": "keyword"},
        "description": {"type": "text", "analyzer": "standard"},
        "extra_metadata": {"type": "object", "enabled": False},
    }
}


def create_indices(es_client: Elasticsearch, recreate: bool = False) -> None:
    indices = [
        (config.INDEX_NAME, PDF_PAGES_SETTINGS, PDF_PAGES_MAPPINGS),
        (config.METADATA_INDEX_NAME, PDF_METADATA_SETTINGS, PDF_METADATA_MAPPINGS),
    ]
    for name, settings, mappings in indices:
        if es_client.indices.exists(index=name):
            if recreate:
                log.info("Deleting existing index: %s", name)
                es_client.indices.delete(index=name)
            else:
                log.info("Index %s already exists, skipping (use --recreate to replace)", name)
                continue
        es_client.indices.create(index=name, settings=settings, mappings=mappings)
        log.info("Created index: %s", name)


def set_refresh_interval(es_client: Elasticsearch, index_name: str, interval: str) -> None:
    es_client.indices.put_settings(index=index_name, settings={"refresh_interval": interval})
    log.info("Set refresh_interval=%s on %s", interval, index_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Elasticsearch indices for PDF search")
    parser.add_argument("--recreate", action="store_true", help="Delete and recreate indices if they exist")
    args = parser.parse_args()

    es = Elasticsearch(config.ES_HOSTS, retry_on_timeout=True, max_retries=3)
    log.info("Connected to ES: %s", es.info()["version"]["number"])
    create_indices(es, recreate=args.recreate)
