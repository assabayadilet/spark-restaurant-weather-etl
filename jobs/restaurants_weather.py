import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("Restaurants PROCESSED - Read Only")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    BASE_PATH = "/data"
    input_restaurant_path = f"{BASE_PATH}/raw/restaurants"
    input_weather_path = f"{BASE_PATH}/raw/Weather"

    df_res = spark.read.parquet(input_restaurant_path)
    weather_dirs = sorted(
        os.path.join(input_weather_path, name)
        for name in os.listdir(input_weather_path)
        if name.startswith("weather")
    )

    if not weather_dirs:
        raise FileNotFoundError(f"No weather directories found in {input_weather_path}")

    df_w = spark.read.parquet(weather_dirs[0])
    for path in weather_dirs[1:]:
        df_w = df_w.unionByName(spark.read.parquet(path))

    print("=== RAW schema ===")
    df_res.printSchema()
    df_w.printSchema()
    
    spark.stop()

if __name__ == "__main__":
    main()
