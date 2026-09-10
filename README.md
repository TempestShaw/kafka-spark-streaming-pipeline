# Kafka Spark Streaming Pipeline

A streaming data pipeline prototype for turning scraped market articles into structured content:

```text
Airflow → Playwright/OpenAI → Kafka → Spark Structured Streaming → Cassandra → Streamlit
```

The repository contains the full Docker-based stack and a small offline demo. The offline demo is the reliable starting point; the live stack still depends on local Docker resources, provider selectors, and an OpenAI API key.

## Offline demo

The demo uses only the Python standard library. It models the important data path with sample messages, URL deduplication, JSONL output, and an HTML preview.

```bash
python demo.py
open demo-output/index.html
```

Expected output:

```text
Normalized 2 articles from 3 input messages.
JSONL: demo-output/blog_posts.jsonl
Preview: demo-output/index.html
```

The duplicate article is removed before the storage-shaped JSONL output is written.

## Live stack

The full stack uses Kafka, Spark, Cassandra, Airflow, and Streamlit:

```bash
cp .env.example .env
# Set OPENAI_API_KEY in .env
docker compose up -d
```

Services expose the following local entry points when healthy:

- Airflow: `http://localhost:8080`
- Streamlit: `http://localhost:8501`
- Kafka Control Center: `http://localhost:9021`
- Spark master UI: `http://localhost:9090`

The live scraper requires the configured website selectors in `dags/utils/scrapping.py`. It should be treated as an experimental integration, not a guaranteed public data feed.

## Repository map

- `dags/kafka_stream.py` — scrape, enrich, and publish article messages.
- `spark_stream.py` — consume Kafka messages and write structured rows to Cassandra.
- `streamlit_app.py` — read stored posts for a simple viewer.
- `dags/playwright_stream.py` — Airflow schedule for the scraper container.
- `demo.py` — no-credential local smoke demo.

## Safety

Keep `.env` and API keys local. The pipeline is for data-engineering experimentation and does not provide investment advice.
