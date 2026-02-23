import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StreamingProcessor")

def create_spark_session() -> SparkSession:
    """Initialize Spark session. In local mode downloads JAR dependencies automatically."""
    kafka_jar_version = "3.5.0" 

    return SparkSession.builder \
        .appName("LiveFootballValueEngine") \
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{kafka_jar_version}") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def main() -> None:
    logger.info("Starting Spark Streaming Processor...")

    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    spark = create_spark_session()

    # Hide Spark logs from JAVA
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema for incoming match events
    match_event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("match_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("event_type", StringType(), False),
        StructField("minute", IntegerType(), False),
        StructField("team_id", StringType(), True),
        StructField("xg_value", DoubleType(), True)
    ])

    # Read streaming data from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", "raw_match_events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse the JSON data and apply the schema
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), match_event_schema).alias("data")
    ).select("data.*")

    # Aggregate xG values
    shots_df = parsed_df.filter(col("event_type").isin("shot", "goal"))

    # Calculate rolling xG over a 5-minute window, updated every minute, 2 min watermark to handle late data
    rolling_xg_df = shots_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("match_id"),
            col("team_id")
        ) \
        .agg(
            _sum("xg_value").alias("rolling_xg")
        )
    
    # Output the rolling xG values to the console for debugging purposes
    query = rolling_xg_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

    # Send the rolling xG values to another Kafka topic for further processing
    kafka_output_df = rolling_xg_df \
        .selectExpr(
            "match_id as key",
            "to_json(struct(window.start as window_start, window.end as window_end, match_id, team_id, rolling_xg)) as value"
        )
    
    kafka_query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", "model_features") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/rolling_xg") \
        .outputMode("update") \
        .start()

    # Wait for both queries to finish (they won't, as they are streaming)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()