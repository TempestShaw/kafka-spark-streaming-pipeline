import os

import requests
import streamlit as st
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def load_posts() -> list[dict]:
    host = os.getenv("CASSANDRA_HOST", "cassandra_db")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    username = os.getenv("CASSANDRA_USERNAME")
    password = os.getenv("CASSANDRA_PASSWORD")
    auth_provider = (
        PlainTextAuthProvider(username, password)
        if username and password
        else None
    )

    cluster = Cluster([host], port=port, auth_provider=auth_provider)
    try:
        session = cluster.connect("spark_streams")
        rows = session.execute("SELECT title, content, image FROM blog_posts")
        return [
            {"title": row.title, "content": row.content, "image": row.image}
            for row in rows
        ]
    finally:
        cluster.shutdown()


def run() -> None:
    st.set_page_config(page_title="Streaming Article Viewer", page_icon="📰")
    st.title("Streaming Article Viewer")
    st.caption("Cassandra-backed output from the Kafka → Spark pipeline")

    try:
        posts = load_posts()
    except Exception as exc:
        st.error("Cassandra is unavailable. Start the pipeline services first.")
        st.caption(f"Connection detail: {exc}")
        return

    if not posts:
        st.info("No articles have reached Cassandra yet.")
        return

    for post in posts:
        st.subheader(post["title"])
        if post["image"]:
            try:
                response = requests.get(post["image"], timeout=10)
                response.raise_for_status()
                st.image(response.content, caption=post["title"])
            except requests.RequestException:
                st.caption("Article image unavailable")
        st.write(post["content"])


if __name__ == "__main__":
    run()
