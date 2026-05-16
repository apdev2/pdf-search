import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from elasticsearch import Elasticsearch, NotFoundError
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

import config
import indexer
import searcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    es = Elasticsearch(config.ES_HOSTS, retry_on_timeout=True, max_retries=3)
    info = es.info()
    log.info("Connected to ES cluster: %s, version %s", info["cluster_name"], info["version"]["number"])
    application.state.es = es
    yield
    es.close()


app = FastAPI(title="PDF Page Search", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
def ui():
    return FileResponse(STATIC_DIR / "index.html", media_type="text/html")


def _es() -> Elasticsearch:
    return app.state.es


# --- Pydantic models for ingestion ---


class PageRecord(BaseModel):
    page_number: int
    text: str


class PDFMetadata(BaseModel):
    total_pages: Optional[int] = None
    file_size_bytes: Optional[int] = None
    tags: Optional[list[str]] = None
    description: Optional[str] = None
    extra_metadata: Optional[dict] = None


class BulkIngestRequest(BaseModel):
    pdf_id: str
    pdf_filename: str
    pages: list[PageRecord]
    metadata: Optional[PDFMetadata] = None


# --- Search endpoints ---


@app.get("/search")
def search_word_endpoint(
    q: str,
    fuzziness: str = "AUTO",
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=500),
):
    return searcher.search_word(_es(), q, fuzziness=fuzziness, page=page, page_size=page_size)


@app.get("/search/phrase")
def search_phrase_endpoint(
    q: str,
    page: int = Query(0, ge=0),
    page_size: int = Query(50, ge=1, le=500),
):
    return searcher.search_phrase(_es(), q, page=page, page_size=page_size)


@app.get("/search/pdf/{pdf_id}")
def search_in_pdf_endpoint(pdf_id: str, q: str, fuzziness: str = "AUTO"):
    return searcher.search_word_in_pdf(_es(), q, pdf_id, fuzziness=fuzziness)


@app.get("/pdf/{pdf_id}/pages")
def get_pdf_pages_endpoint(pdf_id: str, include_text: bool = False):
    return searcher.get_pdf_pages(_es(), pdf_id, include_text=include_text)


@app.get("/pdf/{pdf_id}/pages/{page_number}")
def get_single_page_endpoint(pdf_id: str, page_number: int):
    result = searcher.get_single_page(_es(), pdf_id, page_number)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Page {page_number} of {pdf_id} not found")
    return result


@app.get("/view/{pdf_id}/{page_number}")
def view_page(pdf_id: str, page_number: int):
    return FileResponse(STATIC_DIR / "page.html", media_type="text/html")


# --- Ingestion endpoint ---


@app.post("/ingest/bulk")
def ingest_bulk(req: BulkIngestRequest):
    pages = [
        {
            "pdf_id": req.pdf_id,
            "page_number": p.page_number,
            "text": p.text,
            "pdf_filename": req.pdf_filename,
        }
        for p in req.pages
    ]

    indexed, failed = indexer.index_pages(_es(), iter(pages))

    total_pages = len(req.pages)
    if req.metadata:
        total_pages = req.metadata.total_pages or len(req.pages)
        indexer.upsert_pdf_metadata(
            _es(),
            pdf_id=req.pdf_id,
            pdf_filename=req.pdf_filename,
            total_pages=total_pages,
            file_size_bytes=req.metadata.file_size_bytes,
            tags=req.metadata.tags,
            description=req.metadata.description,
            extra_metadata=req.metadata.extra_metadata,
        )
        _es().indices.refresh(index=config.METADATA_INDEX_NAME)

    return {"pdf_id": req.pdf_id, "indexed_count": indexed, "failure_count": failed}


# --- Metadata endpoints ---


@app.get("/pdfs")
def list_pdfs(
    page: int = Query(0, ge=0),
    page_size: int = Query(20, ge=1, le=100),
    tag: Optional[str] = None,
    sort_by: str = "uploaded_at",
    sort_order: str = "desc",
):
    filters = []
    if tag:
        filters.append({"term": {"tags": tag}})

    query = {"bool": {"filter": filters}} if filters else {"match_all": {}}

    resp = _es().search(
        index=config.METADATA_INDEX_NAME,
        query=query,
        from_=page * page_size,
        size=page_size,
        sort=[{sort_by: sort_order}],
    )

    pdfs = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        src.pop("extra_metadata", None)
        pdfs.append(src)

    return {
        "total": resp["hits"]["total"]["value"],
        "page": page,
        "page_size": page_size,
        "pdfs": pdfs,
    }


@app.get("/pdfs/{pdf_id}")
def get_pdf_metadata(pdf_id: str):
    try:
        meta_resp = _es().get(index=config.METADATA_INDEX_NAME, id=pdf_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"PDF {pdf_id} not found")

    src = meta_resp["_source"]

    pages_resp = _es().search(
        index=config.INDEX_NAME,
        query={"term": {"pdf_id": pdf_id}},
        source=["page_number"],
        size=10_000,
        sort=[{"page_number": "asc"}],
    )

    page_numbers = [hit["_source"]["page_number"] for hit in pages_resp["hits"]["hits"]]

    return {
        "pdf_id": src.get("pdf_id"),
        "pdf_filename": src.get("pdf_filename"),
        "total_pages": src.get("total_pages"),
        "indexed_pages": len(page_numbers),
        "page_numbers": page_numbers,
        "file_size_bytes": src.get("file_size_bytes"),
        "uploaded_at": src.get("uploaded_at"),
        "last_indexed_at": src.get("last_indexed_at"),
        "tags": src.get("tags"),
        "description": src.get("description"),
        "extra_metadata": src.get("extra_metadata"),
    }


@app.delete("/pdfs/{pdf_id}")
def delete_pdf(pdf_id: str):
    pages_result = _es().delete_by_query(
        index=config.INDEX_NAME,
        query={"term": {"pdf_id": pdf_id}},
        refresh=True,
    )
    pages_deleted = pages_result.get("deleted", 0)

    meta_deleted = False
    try:
        _es().delete(index=config.METADATA_INDEX_NAME, id=pdf_id, refresh=True)
        meta_deleted = True
    except NotFoundError:
        pass

    return {
        "pdf_id": pdf_id,
        "pages_deleted": pages_deleted,
        "metadata_deleted": meta_deleted,
    }


# --- Health endpoint ---


@app.get("/health")
def health():
    es = _es()
    cluster = es.cluster.health()

    pages_count = es.count(index=config.INDEX_NAME)["count"] if es.indices.exists(index=config.INDEX_NAME) else 0
    meta_count = es.count(index=config.METADATA_INDEX_NAME)["count"] if es.indices.exists(index=config.METADATA_INDEX_NAME) else 0

    return {
        "cluster_status": cluster["status"],
        "number_of_nodes": cluster["number_of_nodes"],
        "pdf_pages_count": pages_count,
        "pdf_metadata_count": meta_count,
    }
