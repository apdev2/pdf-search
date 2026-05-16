import json
import logging
import sys
import time

import httpx
from elasticsearch import Elasticsearch
from faker import Faker

import config
import indexer
import searcher
from client import PDFSearchClient
from es_setup import create_indices

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)

PASS = "PASS"
FAIL = "FAIL"


def wait_for_es(es: Elasticsearch, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            es.info()
            return True
        except Exception:
            time.sleep(2)
    return False


def generate_fake_pages() -> list[dict]:
    pages = []
    for pdf_idx in range(100):
        pdf_id = f"pdf_{pdf_idx:03d}"
        pdf_filename = f"{pdf_id}.pdf"
        for page_num in range(1, 11):
            text = fake.paragraph(nb_sentences=8)

            if pdf_id == "pdf_001" and page_num == 3:
                text += " elasticsearch is a distributed search engine."
            if pdf_id == "pdf_050" and page_num == 7:
                text += " elasticsearch provides full-text search capabilities."
            if pdf_id == "pdf_010" and page_num == 1:
                text += " This is the quarterly revenue report for FY2024."
            if pdf_id == "pdf_020" and page_num == 5:
                text += " The word elesticsearch is intentionally misspelled."

            pages.append({
                "pdf_id": pdf_id,
                "page_number": page_num,
                "text": text,
                "pdf_filename": pdf_filename,
            })
    return pages


def run_test(name: str, condition: bool) -> bool:
    status = PASS if condition else FAIL
    print(f"  [{status}] {name}")
    return condition


def phase1_direct_es():
    print("\n=== Phase 1: Direct Elasticsearch Tests ===\n")

    es = Elasticsearch(config.ES_HOSTS, retry_on_timeout=True, max_retries=3)

    print("Waiting for ES to be healthy...")
    if not wait_for_es(es):
        print("ES not reachable after 60s — aborting Phase 1.")
        return False

    print("ES is healthy. Creating indices...")
    create_indices(es, recreate=True)

    print("Generating 1000 fake pages (100 PDFs x 10 pages)...")
    pages = generate_fake_pages()

    print("Indexing pages...")
    start = time.time()
    indexed, failed = indexer.index_pages(es, iter(pages))
    elapsed = time.time() - start
    print(f"  Indexed {indexed} docs in {elapsed:.1f}s ({indexed / max(elapsed, 0.001):.0f} docs/sec)")
    print(f"  Failures: {failed}")

    print("Upserting metadata for 100 PDFs...")
    for pdf_idx in range(100):
        pdf_id = f"pdf_{pdf_idx:03d}"
        indexer.upsert_pdf_metadata(
            es,
            pdf_id=pdf_id,
            pdf_filename=f"{pdf_id}.pdf",
            total_pages=10,
            tags=["test"],
            description=f"Test PDF {pdf_id}",
        )
    es.indices.refresh(index=config.METADATA_INDEX_NAME)

    all_passed = True
    print("\nSearch tests:")

    result = searcher.search_word(es, "elasticsearch")
    all_passed &= run_test(
        f"search 'elasticsearch' — expect 2 hits, got {result['total']}",
        result["total"] == 2,
    )

    result = searcher.search_word(es, "elastcsearch")
    all_passed &= run_test(
        f"fuzzy search 'elastcsearch' — expect 2 hits, got {result['total']}",
        result["total"] == 2,
    )

    result = searcher.search_phrase(es, "quarterly revenue report")
    all_passed &= run_test(
        f"phrase search 'quarterly revenue report' — expect 1 hit, got {result['total']}",
        result["total"] == 1,
    )

    result = searcher.search_word_in_pdf(es, "elasticsearch", "pdf_001")
    all_passed &= run_test(
        f"search 'elasticsearch' in pdf_001 — expect 1 hit, got {result['total']}",
        result["total"] == 1,
    )

    result = searcher.get_pdf_pages(es, "pdf_001")
    page_numbers = [h["page_number"] for h in result["hits"]]
    all_passed &= run_test(
        f"get pdf_001 pages — expect 10, got {result['total']}, sorted={page_numbers == list(range(1, 11))}",
        result["total"] == 10 and page_numbers == list(range(1, 11)),
    )

    print("\nMetadata tests:")

    list_resp = es.search(
        index=config.METADATA_INDEX_NAME,
        query={"match_all": {}},
        size=10,
        sort=[{"uploaded_at": "desc"}],
    )
    list_total = list_resp["hits"]["total"]["value"]
    list_returned = len(list_resp["hits"]["hits"])
    all_passed &= run_test(
        f"list PDFs page_size=10 — expect 10 returned, total=100, got {list_returned}/{list_total}",
        list_returned == 10 and list_total == 100,
    )

    tag_resp = es.search(
        index=config.METADATA_INDEX_NAME,
        query={"term": {"tags": "test"}},
        size=0,
    )
    tag_total = tag_resp["hits"]["total"]["value"]
    all_passed &= run_test(
        f"list PDFs with tag='test' — expect 100, got {tag_total}",
        tag_total == 100,
    )

    meta_doc = es.get(index=config.METADATA_INDEX_NAME, id="pdf_001")["_source"]
    pages_resp = es.search(
        index=config.INDEX_NAME,
        query={"term": {"pdf_id": "pdf_001"}},
        size=0,
    )
    indexed_count = pages_resp["hits"]["total"]["value"]
    all_passed &= run_test(
        f"pdf_001 metadata — total_pages={meta_doc.get('total_pages')}, indexed_pages={indexed_count}",
        meta_doc.get("total_pages") == 10 and indexed_count == 10,
    )

    print("\nCluster stats:")
    for idx_name in [config.INDEX_NAME, config.METADATA_INDEX_NAME]:
        stats = es.indices.stats(index=idx_name)
        total_docs = stats["_all"]["primaries"]["docs"]["count"]
        size_bytes = stats["_all"]["primaries"]["store"]["size_in_bytes"]
        print(f"  {idx_name}: {total_docs} docs, {size_bytes / 1024:.1f} KB")

    health = es.cluster.health()
    print(f"  Cluster status: {health['status']}, nodes: {health['number_of_nodes']}")

    return all_passed


def phase2_client_server():
    print("\n=== Phase 2: Client → Server Tests ===\n")

    try:
        httpx.get(f"{config.API_SERVER_URL}/health", timeout=5.0)
    except Exception:
        print(f"API server not reachable at {config.API_SERVER_URL} — skipping Phase 2.")
        return None

    es = Elasticsearch(config.ES_HOSTS, retry_on_timeout=True, max_retries=3)
    create_indices(es, recreate=True)
    es.close()

    client = PDFSearchClient()

    print("Server health:")
    health = client.health()
    print(f"  {json.dumps(health)}")

    print("\nIngesting 5 PDFs via client (20 pages each)...")
    known_word = "xylophonic"
    for i in range(5):
        pdf_id = f"client_pdf_{i:03d}"
        pages = []
        for p in range(1, 21):
            text = fake.paragraph(nb_sentences=6)
            if i == 2 and p == 10:
                text += f" The {known_word} resonance was remarkable."
            pages.append({"page_number": p, "text": text})

        metadata = {
            "total_pages": 20,
            "tags": ["client-test"],
            "description": f"Client test PDF {i}",
        }
        result = client.ingest_pdf(pdf_id, f"{pdf_id}.pdf", pages, metadata=metadata)
        print(f"  {pdf_id}: indexed={result['total_indexed']}, failed={result['total_failed']}")

    all_passed = True
    print("\nVerification:")

    pdfs = client.list_pdfs()
    all_passed &= run_test(
        f"list_pdfs total — expect 5, got {pdfs['total']}",
        pdfs["total"] == 5,
    )

    pdf_detail = client.get_pdf("client_pdf_002")
    all_passed &= run_test(
        f"get_pdf client_pdf_002 — indexed_pages expect 20, got {pdf_detail.get('indexed_pages')}",
        pdf_detail.get("indexed_pages") == 20,
    )

    search_result = client.search(known_word)
    all_passed &= run_test(
        f"search '{known_word}' — expect 1 hit, got {search_result['total']}",
        search_result["total"] == 1,
    )

    return all_passed


if __name__ == "__main__":
    p1 = phase1_direct_es()
    p2 = phase2_client_server()

    print("\n=== Summary ===")
    print(f"  Phase 1 (Direct ES): {PASS if p1 else FAIL}")
    if p2 is None:
        print("  Phase 2 (Client→Server): SKIPPED")
    else:
        print(f"  Phase 2 (Client→Server): {PASS if p2 else FAIL}")

    if not p1 or (p2 is not None and not p2):
        sys.exit(1)

