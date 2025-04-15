# src/processing/jobs/run_analytics.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, split, explode, lower, regexp_replace,
    window, count, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType

def main():
    spark = SparkSession.builder \
        .appName("RunAnalytics") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .getOrCreate()

    # 1. Read from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "text_posts") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Convert binary -> string
    df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

    # 3. Define schema
    schema = StructType([
        StructField("did", StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("text", StringType(), True),
    ])

    # 4. Parse JSON
    df_parsed = df_string.select(
        from_json(col("json_str"), schema).alias("data")
    ).select("data.*")

    # 5. Convert 'createdAt' to timestamp (handle microseconds)
    df_parsed = df_parsed.withColumn(
        "createdAt_ts",
        to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    )

    # ==============
    # MOST COMMON KEYWORDS
    # ==============
    # 6. Basic tokenization example:
    # - Lowercase the text
    # - Remove punctuation (regex)
    # - Split on whitespace
    # - Explode into multiple rows
    df_tokens = df_parsed \
        .withColumn("clean_text", lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn("tokens", split(col("clean_text"), "\\s+")) \
        .select("did", "createdAt_ts", explode(col("tokens")).alias("token"))

    # 7. Filter out empty tokens
    df_tokens = df_tokens.filter(col("token") != "")

    # 8. Windowed count of the most frequent tokens
    # Example: 5-minute window with 2-minute slide (you can adjust the durations)
    df_token_counts = df_tokens \
        .withWatermark("createdAt_ts", "10 minutes") \
        .groupBy(
            window(col("createdAt_ts"), "5 minutes", "2 minutes"),
            col("token")
        ) \
        .count() \
        .orderBy("window", col("count").desc())

    # ==============
    # CATEGORIZE POSTS
    # ==============
    # 9. We can create a simple category column based on keywords presence.
    # For instance:
    #    "gaming" if text mentions "game" or "warthunder"
    #    "politics" if text mentions "trump" or "biden"
    #    "other" otherwise
    #
    # We'll do a direct string search on the original 'clean_text' (or tokens).
    # This is naive—use more robust NLP for better classification.
    df_categorized = df_parsed \
        .withColumn("clean_text", lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn(
            "category",
            when(col("clean_text").like("%game%"), lit("gaming"))
            .when(col("clean_text").like("%warthunder%"), lit("gaming"))
            .when(col("clean_text").like("%trump%"), lit("politics"))
            .when(col("clean_text").like("%biden%"), lit("politics"))
            .otherwise(lit("other"))
        )

    # 10. For demonstration, we can just write both streams (counts + categories) to console

    query_counts = df_token_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .queryName("keyword_counts") \
        .start()

    query_categories = df_categorized.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName("categorized_posts") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
