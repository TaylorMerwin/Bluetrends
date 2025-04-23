#!/usr/bin/env python3
# keyword_trends_job.py

import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StringType
import mysql.connector


def main():
    parser = argparse.ArgumentParser(
        description="Generate keyword trends summary for a given time window and write to keyword_trends table"
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start of the window (inclusive), e.g. '2025-04-22 00:00:00'"
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End of the window (exclusive), e.g. '2025-04-23 00:00:00'"
    )
    args = parser.parse_args()

    # JDBC connection parameters
    jdbc_url = "jdbc:mysql://db:3306/bluetrends"
    jdbc_props = {
        "user": "blueuser",
        "password": "bluepassword",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Build Spark session
    spark = (
        SparkSession.builder
        .appName("KeywordTrendsJob")
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.31")
        .getOrCreate()
    )

    # Load only fresh posts in the time window
    posts_df = (
        spark.read
            .jdbc(url=jdbc_url, table="posts", properties=jdbc_props)
            .filter(
                (F.col("created_at") >= args.start) &
                (F.col("created_at") < args.end)
            )
    )

    # Explode keywords array and prepare for aggregation
    exploded = (
        posts_df
        .select(
            "post_id",
            "sentiment_score",
            F.explode(
                F.from_json(
                    F.col("keywords"), ArrayType(StringType())
                )
            ).alias("keyword")
        )
    )

    # Aggregate counts & avg sentiment by keyword
    trends_df = (
        exploded
        .groupBy("keyword")
        .agg(
            F.count("post_id").alias("post_count"),
            F.avg("sentiment_score").alias("avg_sentiment_score")
        )
        .withColumn("period_start", F.lit(args.start))
        .withColumn("period_end",   F.lit(args.end))
    )

    # Optional: remove existing entries for this period to avoid duplicates
    conn = mysql.connector.connect(
        host="db", user="blueuser", password="bluepassword", database="bluetrends"
    )
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM keyword_trends WHERE period_start = %s", (args.start,)
    )
    conn.commit()
    cursor.close()
    conn.close()

    # Write aggregated trends back to MySQL
    trends_df.write.jdbc(url=jdbc_url, table="keyword_trends", mode="append", properties=jdbc_props)

    spark.stop()


if __name__ == "__main__":
    main()
