# src/processing/jobs/jetstream_consumer.py
import os
import logging
import pandas as pd
import pyarrow
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, udf, coalesce, pandas_udf, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, MapType, NumericType, BooleanType, DoubleType
from better_profanity import profanity
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from transformers import TFAutoModelForSequenceClassification, AutoTokenizer, pipeline, AutoConfig, \
    AutoModelForSequenceClassification

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
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "text_posts") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

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
        (col("created_at") > current_timestamp()) |
        (col("created_at") < expr("current_timestamp() - interval 1 hour")),
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

# initialize once
_pipe = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
)


@pandas_udf(sentiment_schema)
def sentiment_udf(texts: pd.Series) -> pd.DataFrame:

    global _pipe
    results = _pipe(texts.tolist(),
    truncation = True,
    max_length = _pipe.tokenizer.model_max_length
    )

    logger.info(f"Sentiment: {results}")
    return pd.DataFrame({
        "label": [result['label'] for result in results],
        "score": [result['score'] for result in results],
    })



# Function that writes a micro-batch of data to the raw_posts table using environment variables
def process_batch(batch_df, batch_id):

    logger.info(f"Processing batch {batch_id}")

    # Drop rows with empty text
    batch_df = batch_df.filter(col("text").isNotNull() & (col("text") != ""))


    # Enrich the DataFrame with additional columns
    batch_df = (batch_df
        .withColumn("language", lang_udf(col("text")))
                .withColumn("sfw", sfw_udf(col("text")))
                .withColumn("sentiment", sentiment_udf(col("text")))
                .withColumn("sentiment_label", col("sentiment.label"))
                .withColumn("sentiment_score", col("sentiment.score"))
                .drop("sentiment")
                )


    logger.info("🔍 Enriched preview")
    batch_df.select(
        "did", "text", "created_at", "language", "sfw",
        "sentiment_label", "sentiment_score",
    ).show(20, truncate=False)

    logger.info("Writing to posts table")

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://db:3306/bluetrends") \
        .option("dbtable", "posts") \
        .option("user", "blueuser") \
        .option("password", "bluepassword") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

# Write the stream to the console, for testing
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()