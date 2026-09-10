from datetime import datetime
from dotenv import load_dotenv
import prompt
from scrapping import *  # Assuming Scrapper is defined in this module
from utils import *
from prompt import *
import logging
import asyncio
import os

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}
# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


async def scrapping(website):
    scraper = Scraper()
    return await scraper.scrap_from_website(website)


def process_raw_data(website):
    title, content, image_src = asyncio.run(scrapping(website))
    if image_src:
        image_description = call_agents(
            prompt=dataprompt,
            text="请根据图片描述。",
            image_url=image_src,
            model='gpt-4-vision-preview',
            temperature=0.3)

        logging.info(f"Image description: {image_description}")

        redreturn = call_agents(
            prompt=redprompt,
            text=f"""
                标题:{title}
                内容:{content}
                    {image_description}""",
            model='gpt-4-0125-preview',
            temperature=0.3)

        logging.info(f"Red return: {redreturn}")

        data = json_parsing(redreturn)

        title = data["标题"]
        content = data["稿子正文"]
        data = {"title": title, "content": content, "image": image_src}
        return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import logging
    load_dotenv(verbose=True)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    producer = KafkaProducer(
        bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",")],
        max_block_ms=5000,
    )

    try:
        data = process_raw_data(visualCapitalist)
        if data is None:
            logging.warning("No article was produced; skipping Kafka publish")
            return
        producer.send('blog_posts', json.dumps(data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        logging.error(f'An error occured: {e}')
    finally:
        producer.close()


def main():
    stream_data()


if __name__ == "__main__":
    main()
