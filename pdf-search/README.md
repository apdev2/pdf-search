# PDF Page Search System

Full-text search over PDF page text using Elasticsearch.

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11.13

### Setup

1. Start Elasticsearch:
   ```
   docker compose up -d
   ```
   Wait ~30 seconds for ES to become healthy.

2. Create and activate virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install Python dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Create the ES indices:
   ```
   python es_setup.py --recreate
   ```

5. Run the test to verify everything works:
   ```
   python test_with_sample_data.py
   ```

6. Start the API server:
   ```
   uvicorn api:app --host 0.0.0.0 --port 8000
   ```

7. Search:
   ```
   curl "http://localhost:8000/search?q=<word>"
   ```

### Ingest data via the Python client

Option A — from a directory of extracted text:
```
source .venv/bin/activate
python client.py --source-dir ./data
```

Option B — programmatically:
```python
from client import PDFSearchClient
client = PDFSearchClient()
client.ingest_pdf(
    pdf_id="my_doc",
    pdf_filename="my_doc.pdf",
    pages=[{"page_number": 1, "text": "page text here..."}],
    metadata={"total_pages": 50, "tags": ["finance"]}
)
```

### Browse indexed PDFs

```
curl "http://localhost:8000/pdfs?page=0&page_size=20"
curl "http://localhost:8000/pdfs?tag=finance"
curl "http://localhost:8000/pdfs/my_doc"
```

### Client CLI

```
python client.py --health
python client.py --list-pdfs
python client.py --get-pdf my_doc
python client.py --search revenue
```

### Configuration

All settings are in config.py. Key values:
- ES_HOSTS: list of Elasticsearch node addresses
- INDEX_NAME / METADATA_INDEX_NAME: ES index names
- SHARDS: number of primary shards (6 for test, 18 for production)
- REPLICAS: 0 for single node, 1+ for multi-node
- API_SERVER_URL: where the client sends data
- CLIENT_BATCH_SIZE: pages per HTTP request from client

### Scaling to multiple nodes

1. Update docker-compose.yml: remove discovery.type=single-node,
   add discovery.seed_hosts with all node addresses.
2. Update config.py: add new node addresses to ES_HOSTS, set REPLICAS=1.
3. Elasticsearch automatically rebalances shards across nodes.
   No application code changes needed.
