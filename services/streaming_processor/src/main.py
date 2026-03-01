import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum, max as spark_max, min as spark_min, avg, struct, to_json, expr
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
        
        StructField("minute", IntegerType(), True),
        StructField("second", IntegerType(), True),
        StructField("home_goals", IntegerType(), True),
        StructField("away_goals", IntegerType(), True),
        
        StructField("home_xg", DoubleType(), True),
        StructField("away_xg", DoubleType(), True),
        StructField("home_possession", DoubleType(), True),
        StructField("away_possession", DoubleType(), True),
        
        StructField("home_total_shots", IntegerType(), True),
        StructField("away_total_shots", IntegerType(), True),
        StructField("home_shots_on_target", IntegerType(), True),
        StructField("away_shots_on_target", IntegerType(), True),
        StructField("home_shots_off_target", IntegerType(), True),
        StructField("away_shots_off_target", IntegerType(), True),
        StructField("home_shots_inside_box", IntegerType(), True),
        StructField("away_shots_inside_box", IntegerType(), True),
        StructField("home_shots_outside_box", IntegerType(), True),
        StructField("away_shots_outside_box", IntegerType(), True),
        StructField("home_big_chances", IntegerType(), True),
        StructField("away_big_chances", IntegerType(), True),

        StructField("home_corner_kicks", IntegerType(), True),
        StructField("away_corner_kicks", IntegerType(), True),
        StructField("home_offsides", IntegerType(), True),
        StructField("away_offsides", IntegerType(), True),
        StructField("home_free_kicks", IntegerType(), True),
        StructField("away_free_kicks", IntegerType(), True),
        
        StructField("home_passes_pct", DoubleType(), True),
        StructField("away_passes_pct", DoubleType(), True),
        StructField("home_long_passes_pct", DoubleType(), True),
        StructField("away_long_passes_pct", DoubleType(), True),
        StructField("home_passes_final_third_pct", DoubleType(), True),
        StructField("away_passes_final_third_pct", DoubleType(), True),
        StructField("home_crosses_pct", DoubleType(), True),
        StructField("away_crosses_pct", DoubleType(), True),
        
        StructField("home_fouls", IntegerType(), True),
        StructField("away_fouls", IntegerType(), True),
        StructField("home_tackles_pct", DoubleType(), True),
        StructField("away_tackles_pct", DoubleType(), True),
        StructField("home_duels_won", IntegerType(), True),
        StructField("away_duels_won", IntegerType(), True),
        StructField("home_clearances", IntegerType(), True),
        StructField("away_clearances", IntegerType(), True),
        StructField("home_interceptions", IntegerType(), True),
        StructField("away_interceptions", IntegerType(), True),
        StructField("home_yellow_cards", IntegerType(), True),
        StructField("away_yellow_cards", IntegerType(), True),
        StructField("home_red_cards", IntegerType(), True),
        StructField("away_red_cards", IntegerType(), True),
        
        StructField("home_goalkeeper_saves", IntegerType(), True),
        StructField("away_goalkeeper_saves", IntegerType(), True),
        StructField("home_xgot_faced", DoubleType(), True),
        StructField("away_xgot_faced", DoubleType(), True),
        StructField("home_goals_prevented", DoubleType(), True),
        StructField("away_goals_prevented", DoubleType(), True)
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
            window(col("timestamp"), "5 minutes", "10 seconds"),
            col("match_id")
        ) \
        .agg(
            # Current match state
            spark_max(expr("minute * 60 + second")).alias("max_total_seconds"),
            spark_max("home_goals").alias("home_goals"),
            spark_max("away_goals").alias("away_goals"),
            
            # --- Momentum ---
            expr("max_by(home_possession, timestamp) - min_by(home_possession, timestamp)").alias("momentum_home_possession"),
            expr("max_by(away_possession, timestamp) - min_by(away_possession, timestamp)").alias("momentum_away_possession"),
            expr("max_by(home_passes_pct, timestamp) - min_by(home_passes_pct, timestamp)").alias("momentum_home_passes_pct"),
            expr("max_by(away_passes_pct, timestamp) - min_by(away_passes_pct, timestamp)").alias("momentum_away_passes_pct"),
            expr("max_by(home_long_passes_pct, timestamp) - min_by(home_long_passes_pct, timestamp)").alias("momentum_home_long_passes_pct"),
            expr("max_by(away_long_passes_pct, timestamp) - min_by(away_long_passes_pct, timestamp)").alias("momentum_away_long_passes_pct"),
            expr("max_by(home_passes_final_third_pct, timestamp) - min_by(home_passes_final_third_pct, timestamp)").alias("momentum_home_passes_final_third_pct"),
            expr("max_by(away_passes_final_third_pct, timestamp) - min_by(away_passes_final_third_pct, timestamp)").alias("momentum_away_passes_final_third_pct"),
            expr("max_by(home_tackles_pct, timestamp) - min_by(home_tackles_pct, timestamp)").alias("momentum_home_tackles_pct"),
            expr("max_by(away_tackles_pct, timestamp) - min_by(away_tackles_pct, timestamp)").alias("momentum_away_tackles_pct"),
            expr("max_by(home_crosses_pct, timestamp) - min_by(home_crosses_pct, timestamp)").alias("momentum_home_crosses_pct"),
            expr("max_by(away_crosses_pct, timestamp) - min_by(away_crosses_pct, timestamp)").alias("momentum_away_crosses_pct"),
            
            # Goals prevented
            expr("max_by(home_goals_prevented, timestamp) - min_by(home_goals_prevented, timestamp)").alias("momentum_home_goals_prevented"),
            expr("max_by(away_goals_prevented, timestamp) - min_by(away_goals_prevented, timestamp)").alias("momentum_away_goals_prevented"),
            
            # Goal expectancy
            (spark_max("home_xg") - spark_min("home_xg")).alias("momentum_home_xg"),
            (spark_max("away_xg") - spark_min("away_xg")).alias("momentum_away_xg"),
            (spark_max("home_xgot_faced") - spark_min("home_xgot_faced")).alias("momentum_home_xgot_faced"),
            (spark_max("away_xgot_faced") - spark_min("away_xgot_faced")).alias("momentum_away_xgot_faced"),
            
            # Shot metrics
            (spark_max("home_total_shots") - spark_min("home_total_shots")).alias("momentum_home_total_shots"),
            (spark_max("away_total_shots") - spark_min("away_total_shots")).alias("momentum_away_total_shots"),
            (spark_max("home_shots_on_target") - spark_min("home_shots_on_target")).alias("momentum_home_shots_on_target"),
            (spark_max("away_shots_on_target") - spark_min("away_shots_on_target")).alias("momentum_away_shots_on_target"),
            (spark_max("home_shots_inside_box") - spark_min("home_shots_inside_box")).alias("momentum_home_shots_inside_box"),
            (spark_max("away_shots_inside_box") - spark_min("away_shots_inside_box")).alias("momentum_away_shots_inside_box"),
            (spark_max("home_big_chances") - spark_min("home_big_chances")).alias("momentum_home_big_chances"),
            (spark_max("away_big_chances") - spark_min("away_big_chances")).alias("momentum_away_big_chances"),
            
            # Playmaking
            (spark_max("home_corner_kicks") - spark_min("home_corner_kicks")).alias("momentum_home_corners"),
            (spark_max("away_corner_kicks") - spark_min("away_corner_kicks")).alias("momentum_away_corners"),
            (spark_max("home_free_kicks") - spark_min("home_free_kicks")).alias("momentum_home_free_kicks"),
            (spark_max("away_free_kicks") - spark_min("away_free_kicks")).alias("momentum_away_free_kicks"),
            
            # Defensive actions
            (spark_max("home_fouls") - spark_min("home_fouls")).alias("momentum_home_fouls"),
            (spark_max("away_fouls") - spark_min("away_fouls")).alias("momentum_away_fouls"),
            (spark_max("home_duels_won") - spark_min("home_duels_won")).alias("momentum_home_duels_won"),
            (spark_max("away_duels_won") - spark_min("away_duels_won")).alias("momentum_away_duels_won"),
            (spark_max("home_clearances") - spark_min("home_clearances")).alias("momentum_home_clearances"),
            (spark_max("away_clearances") - spark_min("away_clearances")).alias("momentum_away_clearances"),
            (spark_max("home_interceptions") - spark_min("home_interceptions")).alias("momentum_home_interceptions"),
            (spark_max("away_interceptions") - spark_min("away_interceptions")).alias("momentum_away_interceptions"),
            (spark_max("home_yellow_cards") - spark_min("home_yellow_cards")).alias("momentum_home_yellow_cards"),
            (spark_max("away_yellow_cards") - spark_min("away_yellow_cards")).alias("momentum_away_yellow_cards"),
            (spark_max("home_goalkeeper_saves") - spark_min("home_goalkeeper_saves")).alias("momentum_home_gk_saves"),
            (spark_max("away_goalkeeper_saves") - spark_min("away_goalkeeper_saves")).alias("momentum_away_gk_saves")
        )
    
    # For debugging purposes
    # logger.info("Starting console output stream...")

    # console_query = momentum_df.select(
    #     "match_id",
    #     "max_total_seconds",
    #     "home_goals",
    #     "away_goals",
    #     "momentum_home_possession",
    #     "momentum_away_possession"
    # ).writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .option("checkpointLocation", "/app/checkpoints/console_stream") \
    #     .trigger(processingTime="10 seconds") \
    #     .start()

    logger.info("Starting Kafka output stream to 'model_features'...")

    # Prepare the output for Kafka - serialize to JSON and use match_id as key for partitioning
    kafka_output_df = momentum_df.selectExpr(
        "match_id as key",
        """to_json(struct(
            window.start as window_start, 
            window.end as window_end, 
            match_id, 
            
            -- Time & Score
            int(max_total_seconds / 60) as current_minute, 
            int(max_total_seconds % 60) as current_second,
            home_goals, away_goals, 
            
            -- Possession & Passing Momentum
            momentum_home_possession, momentum_away_possession,
            momentum_home_passes_pct, momentum_away_passes_pct,
            momentum_home_long_passes_pct, momentum_away_long_passes_pct,
            momentum_home_passes_final_third_pct, momentum_away_passes_final_third_pct,
            momentum_home_tackles_pct, momentum_away_tackles_pct,
            momentum_home_crosses_pct, momentum_away_crosses_pct,
            
            -- Goalkeeping Momentum
            momentum_home_goals_prevented, momentum_away_goals_prevented,
            
            -- Expected Goals Momentum
            momentum_home_xg, momentum_away_xg,
            momentum_home_xgot_faced, momentum_away_xgot_faced,
            
            -- Shooting Momentum
            momentum_home_total_shots, momentum_away_total_shots,
            momentum_home_shots_on_target, momentum_away_shots_on_target,
            momentum_home_shots_inside_box, momentum_away_shots_inside_box,
            momentum_home_big_chances, momentum_away_big_chances,
            
            -- Playmaking Momentum
            momentum_home_corners, momentum_away_corners,
            momentum_home_free_kicks, momentum_away_free_kicks,
            
            -- Defensive Actions Momentum
            momentum_home_fouls, momentum_away_fouls,
            momentum_home_duels_won, momentum_away_duels_won,
            momentum_home_clearances, momentum_away_clearances,
            momentum_home_interceptions, momentum_away_interceptions,
            momentum_home_yellow_cards, momentum_away_yellow_cards,
            momentum_home_gk_saves, momentum_away_gk_saves
        )) as value"""
    )
    
    kafka_query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", "model_features") \
        .option("checkpointLocation", "/app/checkpoints/kafka_momentum_features") \
        .outputMode("update") \
        .start()

    # logger.info("Starting Parquet output stream for ML training data...")

    # training_df = momentum_df \
    #     .withColumn("window_start", col("window.start")) \
    #     .withColumn("window_end", col("window.end")) \
    #     .withColumn("current_minute", expr("int(max_total_seconds / 60)")) \
    #     .withColumn("current_second", expr("int(max_total_seconds % 60)")) \
    #     .drop("window", "max_total_seconds")
    
    # Using append to save only the closed windows
    # parquet_query = training_df.writeStream \
    #     .format("parquet") \
    #     .partitionBy("match_id") \
    #     .option("path", "/app/data/training_set") \
    #     .option("checkpointLocation", "/app/checkpoints/training_parquet") \
    #     .outputMode("append") \
    #     .start()

    # Wait for both queries to finish (they won't, as they are streaming)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()