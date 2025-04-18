#!/usr/bin/env python3
# batch_process_posts.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import BooleanType, StringType, FloatType, ArrayType
from langdetect import detect, DetectorFactory
from better_profanity import profanity
from keybert import KeyBERT
from transformers import pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('BatchPostProcessor')

# ensure consistent language detection
DetectorFactory.seed = 0

# --- UDF definitions ---
logger.info("Defining UDFs")
lang_udf = udf(lambda t: detect(t) if t else None, StringType())
sfw_udf = udf(lambda t: not profanity.contains_profanity(t or ""), BooleanType())

logger.info("Initializing sentiment pipeline")
sent_pipe = pipeline("sentiment-analysis")
sent_label_udf = udf(lambda t: sent_pipe(t)[0]["label"], StringType())
sent_score_udf = udf(lambda t: float(sent_pipe(t)[0]["score"]), FloatType())

logger.info("Initializing KeyBERT model for keywords")
kw_model = KeyBERT()
def extract_kw_arr(text: str):
    kws = kw_model.extract_keywords(
        text,
        keyphrase_ngram_range=(1, 2),
        stop_words='english'
    )
    return [kw for kw, score in kws]
kw_udf = udf(extract_kw_arr, ArrayType(StringType()))


def main():
    try:
        logger.info("Starting Spark session")
        spark = (
            SparkSession.builder
                .appName("BatchPostProcessor")
                .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
                .getOrCreate()
        )

        # --- JDBC settings (hard‑coded) ---
        jdbc_url = "jdbc:mysql://db:3306/bluetrends"
        props = {
            "user":     "blueuser",
            "password": "bluepassword",
            "driver":   "com.mysql.cj.jdbc.Driver"
        }

        # 1) Load raw_posts
        logger.info("Loading raw_posts table")
        raw_df = spark.read.jdbc(jdbc_url, "raw_posts", properties=props) \
            .withColumnRenamed("post_id", "raw_post_id")
        logger.info(f"raw_posts contains {raw_df.count()} rows")

        # 2) Load already-processed posts
        logger.info("Loading posts table")
        proc_df = spark.read.jdbc(jdbc_url, "posts", properties=props)
        logger.info(f"posts table contains {proc_df.count()} rows")

        # 3) Filter new posts
        logger.info("Filtering new posts to process")
        proc_min = proc_df.select("did", "created_at")
        to_proc = raw_df.join(proc_min, on=["did", "created_at"], how="left_anti")
        new_count = to_proc.count()
        logger.info(f"Found {new_count} new posts to process")
        if new_count == 0:
            logger.info("No new posts to process. Exiting.")
            spark.stop()
            return

        # 4) Enrich with language, SFW, sentiment, keywords
        logger.info("Enriching data")
        enriched = (
            to_proc
            .withColumn("language",        lang_udf(col("text")))
            .withColumn("sfw",             sfw_udf(col("text")))
            .withColumn("sentiment_label", sent_label_udf(col("text")))
            .withColumn("sentiment_score", sent_score_udf(col("text")))
            .withColumn("keywords",        kw_udf(col("text")))
        )

        # 5) Preview
        logger.info("Previewing enriched records")
        enriched.select(
            "did", "created_at", "language", "sfw",
            "sentiment_label", "sentiment_score", "keywords"
        ).show(20, truncate=False)

        # 6) Write to posts
        logger.info("Writing to posts table")
        posts_to_write = enriched.select(
            "did", "text", "created_at", "language", "sfw",
            "sentiment_score", "sentiment_label"
        )
        posts_to_write.write.jdbc(jdbc_url, "posts", mode="append", properties=props)

        # 7) Reload for keyword linking
        logger.info("Reloading posts table to get post_ids")
        all_posts = spark.read.jdbc(jdbc_url, "posts", properties=props)
        new_posts = all_posts.select("post_id", "did", "created_at")
        joined = enriched.join(new_posts, on=["did", "created_at"], how="inner")
        logger.info(f"Preparing {joined.count()} post-keyword records")

        # 8) Explode keywords
        logger.info("Exploding keywords for linking")
        exploded = joined.select(
            "post_id", explode(col("keywords")).alias("keyword_name")
        ).distinct()

        # 9) Insert new keywords
        logger.info("Inserting new keywords into keywords table")
        existing_kw = spark.read.jdbc(jdbc_url, "keywords", properties=props)
        new_kw = (
            exploded.select("keyword_name").distinct()
            .join(existing_kw.select("keyword_name"), on="keyword_name", how="left_anti")
        )
        new_kw_count = new_kw.count()
        logger.info(f"Found {new_kw_count} new keywords")
        if new_kw_count > 0:
            new_kw.write.jdbc(jdbc_url, "keywords", mode="append", properties=props)

        # 10) Reload keywords
        logger.info("Reloading keywords table")
        all_kw = spark.read.jdbc(jdbc_url, "keywords", properties=props)

        # 11) Build and write post_keywords
        logger.info("Building post_keywords linking DataFrame")
        post_kw = (
            exploded.join(all_kw, on="keyword_name", how="inner")
            .select("post_id", "keyword_id").distinct()
        )
        link_count = post_kw.count()
        logger.info(f"Inserting {link_count} records into post_keywords")
        post_kw.write.jdbc(jdbc_url, "post_keywords", mode="append", properties=props)

    except Exception:
        logger.exception("Error during batch processing")
    finally:
        logger.info("Stopping Spark session")
        spark.stop()

if __name__ == "__main__":
    main()
