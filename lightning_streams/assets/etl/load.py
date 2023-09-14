#!/usr/bin/env python

import shutil


from pyspark.sql import (
    functions as F,
    types as T,
    SparkSession as S,
    DataFrame as SparkDF,
)

spark = S.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def load_geo_tbl(geo_folder: str) -> SparkDF:
    """
    Load lat+lon files into sink.
    """
    try:
        # latitude spark data frame
        lat_sdf = spark.read.format("csv").load(
            f"{geo_folder}/*.lon.csv", header=True, inferSchema=True
        )
    except Exception as err:
        print(f"Error loading latitude files: {err}")

    try:
        # longitude spark data frame
        lon_sdf = spark.read.format("csv").load(
            f"{geo_folder}/*.lat.csv", header=True, inferSchema=True
        )
    except Exception as err:
        print(f"Error loading longitude files: {err}")

    geo_sdf = (
        lat_sdf.join(lon_sdf, on="ts_date", how="inner")
        .withColumnRenamed("ts_date", "timestamp")
        .coalesce(1)
        .write.mode("overwrite")
        .parquet("data/Load/geospatial")
    )
    shutil.rmtree(geo_folder)


def load_ene_tbl(load_folder: str) -> SparkDF:
    """
    Stream GOES energy files into sink.
    """
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    energySchema = T.StructType().add("ts_date", "timestamp").add("energy", "double")
    try:
        energy_sdf = (
            spark.readStream.schema(energySchema)
            .option("maxFilesPerTrigger", 1)
            .option("checkpointLocation", f"{load_folder}/checkpoint_r")
            .format("csv")
            .load(load_folder, header=True, inferSchema=True)
            .withColumnRenamed("ts_date", "timestamp")
            .coalesce(1)
            .writeStream.format("parquet")
            .option("path", f"{load_folder}/energy")
            .option("checkpointLocation", f"{load_folder}/checkpoint_w")
            .trigger(processingTime="15 seconds")
            .outputMode("append")
            .toTable("energyTable")
            .awaitTermination()
        )
    except Exception as err:
        print(f"Error streaming energy files: {err}")
