import logging
import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import logging
import colorlog

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(levelname)s:%(name)s:%(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',  # INFO messages will be green
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")

# Test logging
logger.debug('A debug message')
logger.info('An info message')  # This will appear in green
logger.warning('A warning message')
logger.error('An error message')
logger.critical('A critical message')


def create_keyspace(session):
    logging.info("Creating keyspace...")
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")


def create_table(session):
    logging.info("Creating table...")
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.blog_posts (
            title TEXT,
            content TEXT,
            image TEXT PRIMARY KEY);
        """)
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")


def insert_data(session, **kwargs):
    title = kwargs.get('title')
    content = kwargs.get('content')
    image = kwargs.get('image')

    logging.info(f"Inserting data for {title} ...")
    try:
        session.execute("""
            INSERT INTO spark_streams.blog_posts(title, content, image)
                VALUES (%s, %s, %s)
        """, (title, content, image))
        logging.info(f"Data inserted for {title}")
    except Exception as e:
        logging.error(
            f'Could not insert data for {title} due to {e}')


def create_spark_connection():
    logging.info("Creating Spark connection...")
    s_conn = None
    try:
        s_conn = SparkSession \
            .builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
            .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(
            f"Couldn't create the spark session due to exception {e}")
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
            .option('subscribe', 'blog_posts') \
            .option('failOnDataLoss', 'false') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    logging.info("Attempting to create a Cassandra connection...")
    try:
        auth_provider = None
        if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
            auth_provider = PlainTextAuthProvider(
                username=CASSANDRA_USERNAME,
                password=CASSANDRA_PASSWORD,
            )
        cluster = Cluster(
            contact_points=[CASSANDRA_HOST],
            port=CASSANDRA_PORT,
            protocol_version=5,
            auth_provider=auth_provider,
        )

        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully.")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    logging.info("Creating selection DataFrame from Kafka...")
    schema = StructType([
        StructField("title", StringType(), False),
        StructField("content", StringType(), False),
        StructField("image", StringType(), False)
    ])

    try:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        logging.info("Selection DataFrame created successfully.")
    except Exception as e:
        logging.error(f"Failed to create selection DataFrame from Kafka: {e}")
        return None

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = (
            create_selection_df_from_kafka(spark_df)
            if spark_df is not None
            else None
        )
        session = create_cassandra_connection()

        if session is not None and selection_df is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'blog_posts')
                               .start())
            streaming_query.awaitTermination()
