from elasticsearch import Elasticsearch

import config

HIGHLIGHT_CONFIG = {
    "fields": {
        "text": {
            "fragment_size": 150,
            "number_of_fragments": 2,
        }
    }
}

SOURCE_FIELDS = ["pdf_id", "page_number", "pdf_filename"]


def _format_hits(response) -> dict:
    total = response["hits"]["total"]["value"]
    hits = []
    for hit in response["hits"]["hits"]:
        entry = {
            "pdf_id": hit["_source"]["pdf_id"],
            "page_number": hit["_source"]["page_number"],
            "pdf_filename": hit["_source"]["pdf_filename"],
            "score": hit["_score"],
            "highlight": " … ".join(hit.get("highlight", {}).get("text", [])),
        }
        hits.append(entry)
    return {"total": total, "hits": hits}


def search_word(
    es_client: Elasticsearch, word: str, fuzziness: str = "AUTO", page: int = 0, page_size: int = 50
) -> dict:
    resp = es_client.search(
        index=config.INDEX_NAME,
        query={"match": {"text": {"query": word, "fuzziness": fuzziness}}},
        source=SOURCE_FIELDS,
        highlight=HIGHLIGHT_CONFIG,
        from_=page * page_size,
        size=page_size,
    )
    return _format_hits(resp)


def search_phrase(es_client: Elasticsearch, phrase: str, page: int = 0, page_size: int = 50) -> dict:
    resp = es_client.search(
        index=config.INDEX_NAME,
        query={"match_phrase": {"text": phrase}},
        source=SOURCE_FIELDS,
        highlight=HIGHLIGHT_CONFIG,
        from_=page * page_size,
        size=page_size,
    )
    return _format_hits(resp)


def search_word_in_pdf(
    es_client: Elasticsearch, word: str, pdf_id: str, fuzziness: str = "AUTO"
) -> dict:
    resp = es_client.search(
        index=config.INDEX_NAME,
        query={
            "bool": {
                "must": {"match": {"text": {"query": word, "fuzziness": fuzziness}}},
                "filter": {"term": {"pdf_id": pdf_id}},
            }
        },
        source=SOURCE_FIELDS,
        highlight=HIGHLIGHT_CONFIG,
        size=100,
    )
    return _format_hits(resp)


def get_pdf_pages(es_client: Elasticsearch, pdf_id: str, include_text: bool = False) -> dict:
    source = SOURCE_FIELDS + ["text"] if include_text else SOURCE_FIELDS
    resp = es_client.search(
        index=config.INDEX_NAME,
        query={"term": {"pdf_id": pdf_id}},
        source=source,
        sort=[{"page_number": "asc"}],
        size=10_000,
    )
    result = _format_hits(resp)
    if include_text:
        for hit, raw in zip(result["hits"], resp["hits"]["hits"]):
            hit["text"] = raw["_source"].get("text", "")
    return result


def get_single_page(es_client: Elasticsearch, pdf_id: str, page_number: int) -> dict | None:
    doc_id = f"{pdf_id}_page_{page_number}"
    try:
        resp = es_client.get(index=config.INDEX_NAME, id=doc_id)
    except Exception:
        return None
    src = resp["_source"]
    return {
        "pdf_id": src["pdf_id"],
        "page_number": src["page_number"],
        "pdf_filename": src["pdf_filename"],
        "text": src.get("text", ""),
    }
