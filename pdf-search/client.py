import argparse
import logging
import re
import time
from pathlib import Path

import httpx

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class PDFSearchClient:
    def __init__(
        self,
        server_url: str | None = None,
        batch_size: int | None = None,
        max_retries: int | None = None,
    ):
        self.server_url = (server_url or config.API_SERVER_URL).rstrip("/")
        self.batch_size = batch_size or config.CLIENT_BATCH_SIZE
        self.max_retries = max_retries or config.CLIENT_MAX_RETRIES
        self._client = httpx.Client(base_url=self.server_url, timeout=60.0)

    def _post_with_retry(self, path: str, json_body: dict) -> httpx.Response:
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self._client.post(path, json=json_body)
                if resp.status_code < 500:
                    return resp
                last_exc = RuntimeError(f"Server error {resp.status_code}: {resp.text}")
            except httpx.HTTPError as exc:
                last_exc = exc

            if attempt < self.max_retries:
                delay = 2**attempt
                log.warning("Retry %d/%d in %ds", attempt + 1, self.max_retries, delay)
                time.sleep(delay)

        raise last_exc  # type: ignore[misc]

    def ingest_pdf(
        self,
        pdf_id: str,
        pdf_filename: str,
        pages: list[dict],
        metadata: dict | None = None,
    ) -> dict:
        total_indexed = 0
        total_failed = 0
        batches_sent = 0
        num_batches = max(1, (len(pages) + self.batch_size - 1) // self.batch_size)

        for i in range(0, len(pages), self.batch_size):
            batch = pages[i : i + self.batch_size]
            batches_sent += 1

            body: dict = {
                "pdf_id": pdf_id,
                "pdf_filename": pdf_filename,
                "pages": batch,
            }
            if batches_sent == 1 and metadata:
                body["metadata"] = metadata

            try:
                resp = self._post_with_retry("/ingest/bulk", body)
                if resp.status_code >= 400:
                    log.error(
                        "Batch %d/%d for %s failed: %d %s",
                        batches_sent, num_batches, pdf_id, resp.status_code, resp.text,
                    )
                    total_failed += len(batch)
                    continue

                result = resp.json()
                total_indexed += result.get("indexed_count", 0)
                total_failed += result.get("failure_count", 0)
                log.info(
                    "Batch %d/%d for %s: %d pages sent, %d indexed, %d failed",
                    batches_sent, num_batches, pdf_id,
                    len(batch), result.get("indexed_count", 0), result.get("failure_count", 0),
                )
            except Exception:
                log.exception("Batch %d/%d for %s failed after retries", batches_sent, num_batches, pdf_id)
                total_failed += len(batch)

        return {
            "pdf_id": pdf_id,
            "total_indexed": total_indexed,
            "total_failed": total_failed,
            "batches_sent": batches_sent,
        }

    def ingest_directory(self, base_path: str) -> dict:
        base = Path(base_path)
        total_pdfs = 0
        total_pages = 0
        total_failures = 0

        for pdf_dir in sorted(base.iterdir()):
            if not pdf_dir.is_dir():
                continue

            pdf_id = pdf_dir.name
            pdf_filename = pdf_id + ".pdf"
            pages = []

            for page_file in sorted(pdf_dir.iterdir()):
                if not page_file.is_file() or page_file.suffix != ".txt":
                    continue
                match = re.match(r"page_(\d+)\.txt", page_file.name)
                if not match:
                    continue
                pages.append({
                    "page_number": int(match.group(1)),
                    "text": page_file.read_text(encoding="utf-8"),
                })

            if not pages:
                continue

            metadata = {"total_pages": len(pages)}
            result = self.ingest_pdf(pdf_id, pdf_filename, pages, metadata=metadata)
            total_pdfs += 1
            total_pages += result["total_indexed"]
            total_failures += result["total_failed"]
            log.info("Ingested %s (%d pages)", pdf_id, result["total_indexed"])

        return {"total_pdfs": total_pdfs, "total_pages": total_pages, "total_failures": total_failures}

    def list_pdfs(self, page: int = 0, page_size: int = 20, tag: str | None = None) -> dict:
        params: dict = {"page": page, "page_size": page_size}
        if tag:
            params["tag"] = tag
        resp = self._client.get("/pdfs", params=params)
        resp.raise_for_status()
        return resp.json()

    def get_pdf(self, pdf_id: str) -> dict:
        resp = self._client.get(f"/pdfs/{pdf_id}")
        resp.raise_for_status()
        return resp.json()

    def search(self, word: str, fuzziness: str = "AUTO", page: int = 0, page_size: int = 50) -> dict:
        resp = self._client.get("/search", params={"q": word, "fuzziness": fuzziness, "page": page, "page_size": page_size})
        resp.raise_for_status()
        return resp.json()

    def health(self) -> dict:
        resp = self._client.get("/health")
        resp.raise_for_status()
        return resp.json()


if __name__ == "__main__":
    import json

    parser = argparse.ArgumentParser(description="PDF Search client")
    parser.add_argument("--server-url", default=config.API_SERVER_URL)
    parser.add_argument("--source-dir", help="Ingest from directory")
    parser.add_argument("--health", action="store_true", help="Check server health")
    parser.add_argument("--list-pdfs", action="store_true", help="List indexed PDFs")
    parser.add_argument("--get-pdf", metavar="PDF_ID", help="Get metadata for a PDF")
    parser.add_argument("--search", metavar="WORD", help="Search for a word")
    args = parser.parse_args()

    client = PDFSearchClient(server_url=args.server_url)

    if args.health:
        print(json.dumps(client.health(), indent=2))
    elif args.source_dir:
        result = client.ingest_directory(args.source_dir)
        print(json.dumps(result, indent=2))
    elif args.list_pdfs:
        print(json.dumps(client.list_pdfs(), indent=2))
    elif args.get_pdf:
        print(json.dumps(client.get_pdf(args.get_pdf), indent=2))
    elif args.search:
        print(json.dumps(client.search(args.search), indent=2))
    else:
        parser.print_help()
