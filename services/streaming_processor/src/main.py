import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum, max as spark_max, min as spark_min, avg, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

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
    snapshot_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("match_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("event_type", StringType(), False),
        
        StructField("match_minute", IntegerType(), True),
        StructField("match_second", IntegerType(), True),
        
        StructField("home_goals", IntegerType(), True),
        StructField("away_goals", IntegerType(), True),
        
        StructField("home_xg", DoubleType(), True),
        StructField("away_xg", DoubleType(), True),
        StructField("home_possession", DoubleType(), True),
        StructField("away_possession", DoubleType(), True),
        
        StructField("home_total_shots", IntegerType(), True),
        StructField("away_total_shots", IntegerType(), True)
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
        from_json(col("value").cast("string"), snapshot_schema).alias("data")
    ).select("data.*")

    # Filter for stats snapshots
    stats_df = parsed_df.filter(col("event_type") == "stats_snapshot")

    # Calculate momentum features using a rolling window of 5 minutes, updated every minute
    momentum_df = stats_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "5 seconds"),
            col("match_id")
        ) \
        .agg(
            # Current match state
            spark_max("match_minute").alias("current_minute"),
            spark_max("home_goals").alias("home_goals"),
            spark_max("away_goals").alias("away_goals"),
            
            # Ball possession (average over the window)
            avg("home_possession").alias("avg_home_possession"),
            avg("away_possession").alias("avg_away_possession"),
            
            # Momentum in xG over last 5 minutes (Delta)
            (spark_max("home_xg") - spark_min("home_xg")).alias("momentum_home_xg"),
            (spark_max("away_xg") - spark_min("away_xg")).alias("momentum_away_xg"),
            
            # Momentum in shots over last 5 minutes (Delta)
            (spark_max("home_total_shots") - spark_min("home_total_shots")).alias("momentum_home_shots"),
            (spark_max("away_total_shots") - spark_min("away_total_shots")).alias("momentum_away_shots")
        )
    
    logger.info("Starting console output stream...")
    console_query = momentum_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info("Starting Kafka output stream to 'model_features'...")

    # Prepare the output for Kafka - serialize to JSON and use match_id as key for partitioning
    kafka_output_df = momentum_df.selectExpr(
        "match_id as key",
        """to_json(struct(
            window.start as window_start, 
            window.end as window_end, 
            match_id, 
            current_minute, 
            home_goals, away_goals, 
            avg_home_possession, avg_away_possession, 
            momentum_home_xg, momentum_away_xg, 
            momentum_home_shots, momentum_away_shots
        )) as value"""
    )
    
    kafka_query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", "model_features") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/momentum_features") \
        .outputMode("update") \
        .start()

    # Wait for both queries to finish (they won't, as they are streaming)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()