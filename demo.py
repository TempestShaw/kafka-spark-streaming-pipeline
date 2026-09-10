"""Run a dependency-free local demo of the article streaming data shape."""

import argparse
import html
import json
from pathlib import Path


SAMPLE_ARTICLES = [
    {
        "url": "https://example.com/markets/semiconductors",
        "title": "Semiconductor demand improves",
        "content": "A sample article representing the Playwright ingestion stage.",
        "image": "https://example.com/semiconductors.png",
    },
    {
        "url": "https://example.com/markets/semiconductors",
        "title": "Semiconductor demand improves",
        "content": "A duplicate message that should be removed before storage.",
        "image": "https://example.com/semiconductors.png",
    },
    {
        "url": "https://example.com/markets/cloud",
        "title": "Cloud infrastructure outlook",
        "content": "A second sample article representing another Kafka message.",
        "image": "https://example.com/cloud.png",
    },
]


def normalize_articles(articles: list[dict]) -> list[dict]:
    """Keep one complete article per URL, matching the storage contract."""
    seen_urls = set()
    normalized = []
    for article in articles:
        url = article["url"].strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        normalized.append(
            {
                "title": article["title"].strip(),
                "content": article["content"].strip(),
                "image": article["image"].strip(),
                "url": url,
            }
        )
    return normalized


def render_html(articles: list[dict]) -> str:
    cards = "\n".join(
        f"""<article class="card">
  <p class="source">{html.escape(article['url'])}</p>
  <h2>{html.escape(article['title'])}</h2>
  <p>{html.escape(article['content'])}</p>
</article>"""
        for article in articles
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kafka Spark Streaming Pipeline Demo</title>
  <style>
    body {{ max-width: 900px; margin: 40px auto; padding: 0 20px; font: 16px system-ui, sans-serif; background: #0f172a; color: #e2e8f0; }}
    h1 {{ color: #67e8f9; }}
    .meta, .source {{ color: #94a3b8; font-size: 13px; }}
    .card {{ margin: 18px 0; padding: 18px; border: 1px solid #334155; border-radius: 12px; background: #111827; }}
    h2 {{ margin: 8px 0; color: #c4b5fd; }}
  </style>
</head>
<body>
  <h1>Kafka → Spark → Cassandra</h1>
  <p class="meta">Offline demo: scrape-shaped input → deduplication → storage-shaped output.</p>
  {cards}
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("demo-output"))
    args = parser.parse_args()

    articles = normalize_articles(SAMPLE_ARTICLES)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "blog_posts.jsonl").write_text(
        "\n".join(json.dumps(article, ensure_ascii=False) for article in articles) + "\n",
        encoding="utf-8",
    )
    (args.output / "index.html").write_text(render_html(articles), encoding="utf-8")

    print(f"Normalized {len(articles)} articles from {len(SAMPLE_ARTICLES)} input messages.")
    print(f"JSONL: {args.output / 'blog_posts.jsonl'}")
    print(f"Preview: {args.output / 'index.html'}")


if __name__ == "__main__":
    main()
