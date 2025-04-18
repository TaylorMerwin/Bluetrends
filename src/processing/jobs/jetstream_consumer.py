# src/processing/jobs/jetstream_consumer.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, udf, coalesce, date_trunc, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, MapType, NumericType


# Initialize Spark session
spark = SparkSession.builder \
    .appName("JetstreamConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "text_posts") \
    .option("startingOffsets", "earliest") \
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



# Attempt to convert the createdAt string using several ISO8601 patterns:
df_parsed = df_parsed.withColumn(
    "createdAt",
    coalesce(
        to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
)

df_parsed = df_parsed.withColumnRenamed("createdAt", "created_at")


# Function that writes a micro-batch of data to the raw_posts table using environment variables
def write_to_raw_posts(batch_df, batch_id):

    # replace null time stamps with current timestamp
    batch_df = batch_df.withColumn("created_at", coalesce(col("created_at"), current_timestamp()))

    batch_df = batch_df.withColumn(
        "created_at",
        when(
            (col("created_at") > current_timestamp()) |
            (col("created_at") < expr("current_timestamp() - interval 1 hour")),
            current_timestamp()
        )
        .otherwise(col("created_at"))
    )


    # Drop rows with empty text
    batch_df = batch_df.filter(col("text").isNotNull() & (col("text") != ""))

    jdbc_url = os.environ.get("MYSQL_JDBC_URL")  # e.g., "jdbc:mysql://<db_host>:<db_port>/bluetrends"
    username = os.environ.get("MYSQL_USERNAME")
    password = os.environ.get("MYSQL_PASSWORD")
    # print("jdbc url: ", jdbc_url)
    # print("username: ", username)
    # print("password: ", password)
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://db:3306/bluetrends") \
        .option("dbtable", "raw_posts") \
        .option("user", "blueuser") \
        .option("password", "bluepassword") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

# Write the stream to the console, for testing
query = df_parsed.writeStream \
    .foreachBatch(write_to_raw_posts) \
    .outputMode("append") \
    .start()

query.awaitTermination()