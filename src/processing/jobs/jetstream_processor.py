# src/processing/jobs/jetstream_consumer.py
import os
import logging
import pandas as pd
import json
import pyarrow
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, expr, explode, col, to_timestamp, udf, coalesce, pandas_udf, \
    current_timestamp, when, expr, size, to_json, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType, NumericType, BooleanType, DoubleType, \
    ArrayType, LongType
from better_profanity import profanity
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from transformers import TFAutoModelForSequenceClassification, AutoTokenizer, pipeline, AutoConfig, \
    AutoModelForSequenceClassification
from keybert import KeyBERT


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger('JetstreamProcessor')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JetstreamConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.sql.shuffle.partitions", "12") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "text_posts") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()


# At driver startup: load and broadcast existing keywords once
existing_kw_map = {
    row['keyword_name']: row['keyword_id']
    for row in spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://db:3306/bluetrends")
        .option("dbtable", "keywords")
        .option("user", "blueuser")
        .option("password", "bluepassword")
        .load()
        .select("keyword_name", "keyword_id")
        .collect()
}
broadcast_kw_map = spark.sparkContext.broadcast(existing_kw_map)

# (Optional) also broadcast post_uuid → post_id map for faster lookups
posts_map = {
    row['post_uuid']: row['post_id']
    for row in spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://db:3306/bluetrends")
        .option("dbtable", "posts")
        .option("user", "blueuser")
        .option("password", "bluepassword")
        .load()
        .select("post_uuid", "post_id")
        .collect()
}
broadcast_posts_map = spark.sparkContext.broadcast(posts_map)

# Convert binary value column to string
df_string = df.selectExpr("CAST(value AS STRING) as json_str")

# Define the schema matching our JSON structure
schema = StructType([
    StructField("did", StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("text", StringType(), True),
])

# Parse the JSON string column into a structured DataFrame
df_parsed = df_string.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

df_parsed = df_parsed.withColumn("post_uuid", expr("uuid()"))


df_parsed = df_parsed.withColumn(
        "createdAt",
        coalesce(
            to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
            to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
    )

df_parsed = df_parsed.withColumnRenamed("createdAt", "created_at")

# replace null time stamps with current timestamp
df_parsed = df_parsed.withColumn("created_at", coalesce(col("created_at"), current_timestamp()))

df_parsed = df_parsed.withColumn(
    "created_at",
    when(
        (col("created_at") > current_timestamp()) | (col("created_at") < expr("current_timestamp() - interval 1 hour")),
        current_timestamp()
    )
    .otherwise(col("created_at"))
)

# Functions for additional processing

# Set the seed for reproducibility
DetectorFactory.seed = 0

def safe_detect(text: str) -> str | None:
    if not text:
        return None
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"

lang_udf = udf(safe_detect, StringType())

sfw_udf = udf(lambda t: not profanity.contains_profanity(t or ""), BooleanType())

# Sentiment schema
sentiment_schema = StructType([
    StructField("label", StringType(), True),
    StructField("score", DoubleType(), True),
])

# Unified sentiment UDF
tokenizer = AutoTokenizer.from_pretrained(
    "cardiffnlp/twitter-roberta-base-sentiment-latest"
)


# Create a pipeline for sentiment analysis
_pipe = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    tokenizer=tokenizer
)

@pandas_udf(sentiment_schema)
def sentiment_udf(texts: pd.Series) -> pd.DataFrame:
    batch = texts.fillna("").tolist()
    # pipeline(...) returns a list of {"label":…, "score":…} dicts
    results = _pipe(
      batch,
      padding=True,
      truncation=True,
      max_length=512
    )
    return pd.DataFrame({
      "label": [r["label"] for r in results],
      "score": [r["score"] for r in results],
    })

# Keyword detection and extraction
kw_model = KeyBERT(model="all-MiniLM-L6-v2")

custom_stop_words = [
    # very common English words
    'a','an','the','and','or','but','if','then','else','for','while',
    'of','at','by','with','without','in','on','to','from','up','down',
    'is','are','be','been','being','it','its','this','that','these','those',
    'i','you','he','she','they','we','us','our','their','me','my','mine',
    # contractions & filler
    'im','ive','dont','cant','wont','just','really','also','very','so',
    'only','even','still','yet','much','many','more','some','any','no','not',
    # Bluesky-specific noise
    'thank','thanks','bsky','social', 'bluesky', 'exactly','sexy','love','donation', 'donations',
    'donate','donating', 'help','helping', 'youtube', 'video', 'videos', 'twitch', 'exactly',
]


@pandas_udf(StringType())
def extract_keywords_json_udf(text_series: pd.Series) -> pd.Series:

    # Convert to list of strings
    texts = text_series.fillna("").tolist()
    results = []
    for text in texts:
        if text:
            # Extract keywords using KeyBERT
            keywords = (kw_model.extract_keywords(
                text,
                keyphrase_ngram_range=(1, 2),
                stop_words=custom_stop_words,
                top_n=3
            ))
            # Convert to JSON string
            json_str = json.dumps(keywords)
            results.append(json_str)
        else:
            results.append("[]")

    return pd.Series(results)


def enrich_batch(batch_df):

    # Drop rows with empty text
    batch_df = batch_df.filter(col("text").isNotNull() & (col("text") != ""))

    # Enrich the DataFrame
    batch_df = (
        batch_df
        .withColumn("language", lang_udf(col("text")))
        .withColumn("sfw",      sfw_udf(col("text")))
        .withColumn("sentiment", sentiment_udf(col("text")))
        .withColumn("sentiment_label", col("sentiment.label"))
        .withColumn("sentiment_score", col("sentiment.score"))
        .drop("sentiment")
        .withColumn("keywords", extract_keywords_json_udf(col("text")))

    )

    # Only extract keywords for English & SFW posts
    batch_df = batch_df.withColumn(
        "keywords",
        when(
            (col("language") == "en") & (col("sfw") == True),
            extract_keywords_json_udf(col("text"))
        ).otherwise(lit("[]"))
    )
    return batch_df


def write_posts_table(enriched_df):
    """Write the core post fields out to MySQL."""

    # posts_df = enriched_df.drop("keywords")

    posts_df = (
        enriched_df
        .select(
            "post_uuid", "did", "created_at", "text",
            "language", "sfw", "sentiment_label", "sentiment_score", "keywords"
        )
    )

    posts_df.write \
        .format("jdbc") \
        .option("url",      "jdbc:mysql://db:3306/bluetrends") \
        .option("dbtable",  "posts") \
        .option("user",     "blueuser") \
        .option("password", "bluepassword") \
        .option("driver",   "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()


def process_batch(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    enriched_df = enrich_batch(batch_df)
    # DEBUG: inspect a few keyword outputs
    enriched_df.select("text", "keywords").show(5, truncate=False)
    write_posts_table(enriched_df)


# Write the stream to the console, for testing
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()