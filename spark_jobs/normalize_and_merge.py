"""
PySpark job to normalize and merge Spotify data from GCS
and write back to GCS as Parquet.

This job is submitted to Dataproc by Airflow.
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Creates and returns a SparkSession."""
    return (
        SparkSession.builder.appName("SpotifyETLNormalization")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def process_recently_played(spark, input_path):
    """
    Processes the 'recently_played' JSONL data.
    Flattens the nested structure.
    """
    logger.info(f"Processing recently_played data from {input_path}")

    # Define the complex schema for 'track'
    track_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField(
                "artists",
                StructType(
                    [StructField("id", StringType(), True), StructField("name", StringType(), True)]
                ),
                True,
            ),
            StructField(
                "album",
                StructType(
                    [StructField("id", StringType(), True), StructField("name", StringType(), True)]
                ),
                True,
            ),
        ]
    )

    # Define the main schema for the 'recently_played' item
    main_schema = StructType(
        [
            StructField("track", track_schema, True),
            StructField("played_at", StringType(), True),
            StructField(
                "context", StructType([StructField("uri", StringType(), True)]), True
            ),
        ]
    )

    df = spark.read.schema(main_schema).json(input_path)
    
    # Flatten the structure
    df_flat = df.select(
        col("track.id").alias("track_id"),
        col("track.name").alias("track_name"),
        col("track.artists.id").alias("artist_id"), # Assumes first artist
        col("track.artists.name").alias("artist_name"), # Assumes first artist
        col("track.album.id").alias("album_id"),
        col("track.album.name").alias("album_name"),
        col("context.uri").alias("context_uri"),
        to_timestamp(col("played_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("played_at_ts"),
    )
    
    # Add date partition column
    df_final = df_flat.withColumn("played_at_day", date_format(col("played_at_ts"), "yyyy-MM-dd"))
    
    logger.info("Finished processing recently_played data.")
    df_final.printSchema()
    
    return df_final


def main(args):
    """
    Main ETL job function.
    """
    spark = get_spark_session()

    # Process main fact data (recently played)
    df_listens = process_recently_played(spark, args.input_plays_path)

    
    logger.info(f"Writing final processed data to {args.output_path}")

    # Write the data to the processing bucket as Parquet, partitioned by date
    # This will be read by dbt
    df_listens.write.mode("overwrite").partitionBy("played_at_day").parquet(args.output_path)
    
    logger.info("Spark job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_plays_path", required=True, help="GCS path to recently_played JSONL files."
    )
    parser.add_argument(
        "--input_artists_path", required=False, help="GCS path to artists JSONL files."
    )
    parser.add_argument(
        "--input_tracks_path", required=False, help="GCS path to tracks JSONL files."
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="GCS output path for processed Parquet files.",
    )
    
    known_args, _ = parser.parse_known_args()
    main(known_args)
