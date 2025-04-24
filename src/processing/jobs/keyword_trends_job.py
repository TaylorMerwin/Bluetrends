#!/usr/bin/env python3
import argparse
import logging
import mysql.connector
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType


def upsert_trends(trends_df, start, end):
    """Collect trends DataFrame and upsert into MySQL."""
    rows = trends_df.select(
        "keyword", "period_start", "period_end",
        "post_count", "avg_sentiment_score",
        "positive_count", "neutral_count", "negative_count"
    ).collect()

    conn = mysql.connector.connect(
        host="db",
        user="blueuser",
        password="bluepassword",
        database="bluetrends"
    )
    cur = conn.cursor()

    sql = """
    INSERT INTO keyword_trends
      (keyword, period_start, period_end, post_count, avg_sentiment_score,
       positive_count, neutral_count, negative_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      period_end          = VALUES(period_end),
      post_count          = VALUES(post_count),
      avg_sentiment_score = VALUES(avg_sentiment_score),
      positive_count      = VALUES(positive_count),
      neutral_count       = VALUES(neutral_count),
      negative_count      = VALUES(negative_count)
    """

    params = [
        (
            row.keyword,
            row.period_start.strftime("%Y-%m-%d %H:%M:%S"),
            row.period_end.strftime("%Y-%m-%d %H:%M:%S"),
            int(row.post_count),
            float(row.avg_sentiment_score),
            int(row.positive_count),
            int(row.neutral_count),
            int(row.negative_count)
        ) for row in rows
    ]

    logging.info(f"[Debug] upserting {len(params)} rows for window {start}")

    cur.executemany(sql, params)
    conn.commit()
    cur.close()
    conn.close()


def main(start: str, end: str):
    # Initialize Spark
    spark = (
        SparkSession.builder
        .appName("KeywordTrendsJob")
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
        .getOrCreate()
    )
    jdbc_url = "jdbc:mysql://db:3306/bluetrends"

    # Read & filter posts
    posts = (
        spark.read.format("jdbc")
             .option("url",      jdbc_url)
             .option("dbtable",  "posts")
             .option("user",     "blueuser")
             .option("password", "bluepassword")
             .option("driver",   "com.mysql.cj.jdbc.Driver")
             .load()
             .filter((F.col("created_at") >= F.lit(start)) &
                     (F.col("created_at") <  F.lit(end)))
    )

    # Parse keywords JSON
    kw_schema = ArrayType(ArrayType(StringType()))
    posts2 = posts.withColumn(
        "kw_array", F.from_json(F.col("keywords").cast("string"), kw_schema)
    ).select(
        "post_id", "created_at", "sentiment_score", "sentiment_label", "kw_array"
    )

    # Explode into (post, keyword) rows
    exploded = posts2.select(
        "post_id", "created_at", "sentiment_score", "sentiment_label",
        F.explode("kw_array").alias("kv")
    )

    # Extract keyword & filter length
    keyword_rows = exploded.select(
        "post_id", "created_at", "sentiment_score", "sentiment_label",
        F.col("kv").getItem(0).alias("keyword")
    ).filter(F.length("keyword") <= 100)

    # Basic aggregations: count & avg
    base = keyword_rows.groupBy("keyword").agg(
        F.to_timestamp(F.lit(start), "yyyy-MM-dd HH:mm:ss").alias("period_start"),
        F.to_timestamp(F.lit(end),   "yyyy-MM-dd HH:mm:ss").alias("period_end"),
        F.countDistinct("post_id").alias("post_count"),
        F.avg("sentiment_score").alias("avg_sentiment_score")
    )

    # Pivot by explicit sentiment labels
    label_counts = (
        keyword_rows
        .groupBy("keyword")
        .pivot("sentiment_label", ["Positive", "Neutral", "Negative"])
        .agg(F.countDistinct("post_id"))
        .na.fill(0)
    )

    # Join base + label_counts and rename
    trends = (
        base.join(label_counts, on="keyword", how="left")
        .withColumnRenamed("Positive", "positive_count")
        .withColumnRenamed("Neutral",  "neutral_count")
        .withColumnRenamed("Negative", "negative_count")
    )

    # Sanity-check
    logging.info("[Debug] sample trends with label counts:")
    trends.select(
        "keyword", "post_count", "avg_sentiment_score",
        "positive_count", "neutral_count", "negative_count"
    ).show(5, truncate=False)

    # Drop any accidental duplicates
    trends = trends.dropDuplicates(["keyword", "period_start"] )

    # Upsert into MySQL
    upsert_trends(trends, start, end)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True,
                        help="Inclusive window start: 'YYYY-MM-DD HH:MM:SS'" )
    parser.add_argument("--end",   required=True,
                        help="Exclusive window end:   'YYYY-MM-DD HH:MM:SS'" )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] KeywordTrendsJob: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    main(args.start, args.end)
