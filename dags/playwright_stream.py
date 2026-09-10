from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

import logging
import os

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}
# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = DockerOperator(
        task_id='streaming_task',
        image='rtstreaming/playwright-scraper:local',
        api_version='auto',
        auto_remove='success',
        command="python kafka_stream.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="rtstreaming_confluent",
        environment={
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", ""),
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "broker:29092"
            ),
        },
        mount_tmp_dir=False,
        retries=3,
    )
