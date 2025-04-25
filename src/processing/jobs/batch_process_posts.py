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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('BatchPostProcessor')

DetectorFactory.seed = 0


logger.info("Defining UDFs")
lang_udf       = udf(lambda t: detect(t) if t else None, StringType())
sfw_udf        = udf(lambda t: not profanity.contains_profanity(t or ""), BooleanType())

logger.info("Initializing sentiment pipeline")
sent_pipe       = pipeline("sentiment-analysis")
sent_label_udf  = udf(lambda t: sent_pipe(t)[0]["label"],   StringType())
sent_score_udf  = udf(lambda t: float(sent_pipe(t)[0]["score"]), FloatType())

logger.info("Initializing KeyBERT for keyword extraction")
kw_model = KeyBERT()
def extract_kw_arr(text: str):
    kws = kw_model.extract_keywords(
        text,
        keyphrase_ngram_range=(1, 2),
        top_n=5
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

        jdbc_url = "jdbc:mysql://db:3306/bluetrends"
        props = {
            "user":     "blueuser",
            "password": "bluepassword",
            "driver":   "com.mysql.cj.jdbc.Driver"
        }

        logger.info("Loading raw_posts")
        raw_df = (
            spark.read
                 .jdbc(jdbc_url, "raw_posts", properties=props)
                 .withColumnRenamed("post_id", "raw_post_id")
        )

        logger.info("Loading processed posts (raw_post_id only)")
        proc_ids = (
            spark.read
                 .jdbc(jdbc_url, "posts", properties=props)
                 .select("raw_post_id")
        )

        logger.info("Filtering NEW posts to process")
        to_proc = raw_df.join(proc_ids, on="raw_post_id", how="left_anti")
        new_count = to_proc.count()
        logger.info(f"Found {new_count} new posts")
        if new_count == 0:
            logger.info("No new posts. Exiting.")
            spark.stop()
            return

        logger.info("Enriching data")
        enriched = (
            to_proc
              .withColumn("language",        lang_udf(col("text")))
              .withColumn("sfw",             sfw_udf(col("text")))
              .withColumn("sentiment_label", sent_label_udf(col("text")))
              .withColumn("sentiment_score", sent_score_udf(col("text")))
              .withColumn("keywords",        kw_udf(col("text")))
        )

        logger.info("🔍 Enriched preview")
        enriched.select(
            "raw_post_id", "did", "created_at", "language", "sfw",
            "sentiment_label", "sentiment_score", "keywords"
        ).show(20, truncate=False)

        logger.info("Writing to posts table")
        posts_to_write = enriched.select(
            "raw_post_id", "did", "text", "created_at",
            "language", "sfw", "sentiment_score", "sentiment_label"
        )
        posts_to_write.write.jdbc(
            url=jdbc_url,
            table="posts",
            mode="append",
            properties=props
        )

        all_posts = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "posts") \
            .option("user", props["user"]) \
            .option("password", props["password"]) \
            .option("isolationLevel", "READ_COMMITTED") \
            .load()
        id_map = all_posts.select("post_id", "raw_post_id")

        logger.info("🦖🦖🦖🦖id_map preview (should include the 3 new raw_post_ids)🦖🦖🦖🦖")
        id_map.show(truncate=False)
        logger.info(f"id_map.count() = {id_map.count()}")

        logger.info("—— SCHEMA OF enriched ——🍁🍁🍁🍁🍁")
        enriched.printSchema()

        logger.info("—— SCHEMA OF id_map ——🍁🍁🍁🍁")
        id_map.printSchema()

        logger.info("🔍 Joined preview (post_id lookup)")
        joined = enriched.join(id_map, on="raw_post_id", how="inner")
        logger.info("🔍 joined preview (post_id + keywords)")
        joined.select("post_id", "raw_post_id", "keywords").show(truncate=False)

        logger.info("Exploding keywords for linking")
        exploded = joined.select(
            "post_id",
            explode(col("keywords")).alias("keyword_name")
        ).distinct()

        logger.info("🔍 exploded keywords preview 🤯🤯🤯🤯")
        exploded.show(truncate=False)
        logger.info(f"exploded.count() = {exploded.count()}")

        logger.info("Checking for new keywords")
        existing_kw = spark.read.jdbc(jdbc_url, "keywords", properties=props)

        new_kw = exploded.select("keyword_name").distinct() \
            .join(existing_kw.select("keyword_name"),
                  on="keyword_name", how="left_anti")

        logger.info("🔍 new_kw (what will be inserted into keywords table)🤯🤯")

        new_kw.show(truncate=False)

        logger.info(f"new_kw_count = {new_kw.count()}")
        new_kw_count = new_kw.count()
        logger.info(f"Found {new_kw_count} new keywords")
        if new_kw_count > 0:
            new_kw.write.jdbc(jdbc_url, "keywords", mode="append", properties=props)

        logger.info("Reloading keywords table")
        all_kw = spark.read.jdbc(jdbc_url, "keywords", properties=props)

        logger.info("Building post_keywords linking DataFrame")
        post_kw = (
            exploded.join(all_kw, on="keyword_name", how="inner")
                    .select("post_id", "keyword_id")
                    .distinct()
        )
        link_count = post_kw.count()
        logger.info(f"Inserting {link_count} keyword links")

        if link_count > 0:
            post_kw.write.jdbc(
                url=jdbc_url,
                table="post_keywords",
                mode="append",
                properties=props
            )

    except Exception:
        logger.exception("Error during batch processing")
    finally:
        logger.info("Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    main()
