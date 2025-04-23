# src/processing/jobs/jetstream_consumer.py
import os
import logging
import pandas as pd
import pyarrow
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, expr, explode, col, to_timestamp, udf, coalesce, pandas_udf, \
    current_timestamp, when, expr, size
from pyspark.sql.types import StructType, StructField, StringType, MapType, NumericType, BooleanType, DoubleType, \
    ArrayType
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
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.sql.shuffle.partitions", "12") \
    .getOrCreate()

spark.conf.set("spark.python.worker.reuse", "true")

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

@pandas_udf(ArrayType(StringType()))
def extract_keywords_udf(texts: pd.Series) -> pd.Series:
    def extract(text: str) -> list[str]:
        if not isinstance(text, str) or not text.strip():
            return []
        # 1) extract with MMR (or switch to use_maxsum)
        raw_kws = kw_model.extract_keywords(
            text,
            keyphrase_ngram_range=(1,3),
            stop_words="bsky, bluesky",
            use_mmr=True,         # ← or use_maxsum=True
            diversity=0.5,        # only for MMR
            top_n=5               # get the top‑5 candidates
        )
        # 2) filter by score threshold
        filtered = [ (kw, sc) for kw, sc in raw_kws if sc >= 0.5 ]
        # 3) take your final top‑3 (or fewer)
        final = []
        for kw, sc in filtered[:3]:
            trimmed = kw[:100]
            if trimmed:
                final.append(trimmed)
        return final

    return texts.fillna("").apply(extract)

# Function that writes a micro-batch of data to the raw_posts table using environment variables
def process_batch(batch_df, batch_id):

    logger.info(f"Processing batch {batch_id}")

    # Drop rows with empty text
    batch_df = batch_df.filter(col("text").isNotNull() & (col("text") != ""))


    # Enrich the DataFrame with additional columns
    batch_df = (
        batch_df
        .withColumn("language", lang_udf(col("text")))
        .withColumn("sfw", sfw_udf(col("text")))
        .withColumn("sentiment", sentiment_udf(col("text")))
        .withColumn("sentiment_label", col("sentiment.label"))
        .withColumn("sentiment_score", col("sentiment.score"))
        .drop("sentiment")
        )

    # integrate keywords. Only extract keywords for English posts and posts with no profanity
    #batch_df = batch_df.withColumn("keywords", extract_keywords_udf(col("text")))

    batch_df = batch_df.withColumn(
        "keywords",
        when(
            (col("language") == "en") & (col("sfw") == True),
            extract_keywords_udf(col("text"))
        ).otherwise(expr("array()")) # empty array for non-English or profane posts
    )

    # Flatten keyword arrays into one row per (post, keyword)
    # kw_pairs = batch_df.select(
    #     col("post_uuid"),
    #     explode(col("keywords")).alias("keyword_name")
    # ).dropDuplicates()

    kw_pairs = batch_df.filter(size(col("keywords")) > 0).select(col("post_uuid"),
        explode(col("keywords")).alias("keyword_name")).dropDuplicates()

    # Drop temp keywords column
    posts_write_df = batch_df.drop("keywords")


    logger.info("🔍 Enriched preview")
    posts_write_df.select(
        "did", "text", "created_at", "language", "sfw",
        "sentiment_label", "sentiment_score",
    ).show(20, truncate=False)

    logger.info("Writing to posts table")
    posts_write_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://db:3306/bluetrends") \
        .option("dbtable", "posts") \
        .option("user", "blueuser") \
        .option("password", "bluepassword") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

    #  Upsert keywords and link to posts using Python connector
    logger.info("Upserting keywords and post_keywords")

    conn = mysql.connector.connect(
        host="db",
        user="blueuser",
        password="bluepassword",
        database="bluetrends"
    )
    cursor = conn.cursor()

    # Upsert unique keywords
    # unique_keywords = [row.keyword_name for row in kw_pairs.select("keyword_name").distinct().collect()]
    # --- Trim keywords to fit VARCHAR(100) ---
    # max_kw_len = 100
    # unique_keywords = [
    #     kw.strip().lower()[:max_kw_len]
    #     for kw in (
    #         row.keyword_name
    #         for row in kw_pairs.select("keyword_name").distinct().collect()
    #     )
    # ]
    #
    # for kw in unique_keywords:
    #     cursor.execute(
    #         "INSERT INTO keywords (keyword_name) VALUES (%s)"
    #         " ON DUPLICATE KEY UPDATE keyword_id = keyword_id", (kw,)
    #     )

    unique_keywords = [
        kw.strip().lower()[:100]
        for kw in (
            row.keyword_name
            for row in kw_pairs.select("keyword_name").distinct().collect()
        )
    ]
    # Build list of single-element tuples for executemany
    kw_rows = [(kw,) for kw in unique_keywords]
    cursor.executemany(
        """
        INSERT INTO keywords (keyword_name)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE keyword_id = keyword_id
        """,
        kw_rows
    )
    conn.commit()

    logger.info(f"▶ unique_keywords ({len(unique_keywords)}): {unique_keywords}")

    # Build mapping from keyword_name -> keyword_id
    # cursor.execute("SELECT keyword_id, keyword_name FROM keywords"
    #                " WHERE keyword_name IN (%s)" % ','.join(['%s'] * len(unique_keywords)),
    #                unique_keywords
    #                )

    # use the same unique_keywords list for the IN clause
    # placeholder = ','.join(['%s'] * len(unique_keywords))
    # cursor.execute(
    #     f"SELECT keyword_id, keyword_name FROM keywords WHERE keyword_name IN ({placeholder})",
    # unique_keywords
    # )
    #
    # kw_map = { name: kid for kid, name in cursor.fetchall() }

    # Build mapping from keyword_name -> keyword_id (only for new keywords)

    if unique_keywords:
        placeholder = ','.join(['%s'] * len(unique_keywords))
        cursor.execute(f"SELECT keyword_id, keyword_name FROM keywords WHERE keyword_name IN ({placeholder})", unique_keywords)
        kw_map = {name: kid for kid, name in cursor.fetchall()}
    else:
        kw_map = {}


    # # build mapping from post_uuid -> post_id
    # uuids = [row.post_uuid for row in batch_df.select("post_uuid").distinct().collect()]
    # cursor.execute(
    #     "SELECT post_id, post_uuid FROM posts"
    #     " WHERE post_uuid IN (%s)" % ','.join(['%s'] * len(uuids)),
    #     uuids
    # )
    # post_map = {uuid: pid for pid, uuid in cursor.fetchall()}

    uuids = [row.post_uuid for row in batch_df.select("post_uuid").distinct().collect()]

    if uuids:
        placeholder = ','.join(['%s'] * len(uuids))
        cursor.execute(f"SELECT post_id, post_uuid FROM posts WHERE post_uuid IN ({placeholder})", uuids)
        post_map = {uuid: pid for pid, uuid in cursor.fetchall()}
    else:
        post_map = {}

    # --- Bulk insert into post_keywords in one go ---
    pk_rows = []
    for row in kw_pairs.collect():
        pid = post_map.get(row.post_uuid)
        kid = kw_map.get(row.keyword_name)
        if pid is not None and kid is not None:
            pk_rows.append((pid, kid))

    if pk_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO post_keywords (post_id, keyword_id)
            VALUES (%s, %s)
            """,
            pk_rows
        )
        conn.commit()


    # logger.info(f"▶ kw_map.keys() ({len(kw_map)}): {list(kw_map.keys())}")
    # missing = set(unique_keywords) - set(kw_map.keys())
    # if missing:
    #     logger.warning(f"⚠️ Keywords in `unique_keywords` but not in `kw_map`: {missing}")
    #
    # # build mapping from post_uuid -> post_id
    # uuids = [row.post_uuid for row in batch_df.select("post_uuid").distinct().collect()]
    # cursor.execute(
    #     "SELECT post_id, post_uuid FROM posts"
    #     " WHERE post_uuid IN (%s)" % ','.join(['%s'] * len(uuids)),
    #     uuids
    # )
    # post_map = { uuid: pid for pid, uuid in cursor.fetchall() }
    #
    # # Insert into post_keywords bridge table
    # for row in kw_pairs.collect():
    #     pid = post_map[row.post_uuid]
    #
    #     kid = kw_map.get(row.keyword_name)
    #     if kid is None:
    #         logger.warning(f"⚠️ Keyword '{row.keyword_name}' not found in kw_map")
    #         continue
    #
    #     cursor.execute(
    #         "INSERT IGNORE INTO post_keywords (post_id, keyword_id) VALUES (%s,%s)",
    #         (pid, kid)
    #     )

    cursor.close()
    conn.close()

# Write the stream to the console, for testing
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()