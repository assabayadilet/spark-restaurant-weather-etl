from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests

def main():
    spark = (
        SparkSession.builder
        .appName("Restaurants PROCESSED - Read Only")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    BASE_PATH = "/opt/spark-data"
    input_path = f"{BASE_PATH}/raw/restaurants"

    df = spark.read.parquet(input_path)

    print("=== RAW schema ===")
    df.printSchema()

    total_count = df.count()

    invalid_coords_df = df.filter(
        (col("lat").isNull()) |
        (col("lng").isNull()) |
        (col("lat") == 0) |
        (col("lng") == 0)
    ).select("id", "city", "country")

    invalid_count = invalid_coords_df.collect()

    print(f"Total restaurants: {total_count}")
    print(f"Restaurants with invalid coordinates: {invalid_count}")

    if invalid_count > 0:
        print("=== Sample invalid coordinate rows ===")
        invalid_coords_df.show(5, truncate=False)
    
    import requests
    import os

    OPENCAGE_KEY = os.getenv("OPENCAGE_API_KEY")

    def geocode(city, country):
        query = f"{city}, {country}"
        url = "https://api.opencagedata.com/geocode/v1/json"
        params = {
            "q": query,
            "key": OPENCAGE_KEY,
            "limit": 1
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    if data["results"]:
        lat = data["results"][0]["geometry"]["lat"]
        lng = data["results"][0]["geometry"]["lng"]
        return lat, lng

    return None, None

    fixed_rows = []

    for row in invalid_rows:
        lat, lng = geocode(row.city, row.country)
        if lat and lng:
            fixed_rows.append((row.id, lat, lng))

    from pyspark.sql.types import StructType, StructField, LongType, DoubleType

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("fixed_lat", DoubleType(), True),
        StructField("fixed_lng", DoubleType(), True),
    ])

    df_enriched = (
        df
        .join(fixed_df, on="id", how="left")
        .withColumn(
            "lat",
            col("fixed_lat").when(col("fixed_lat").isNotNull(), col("fixed_lat")).otherwise(col("lat"))
        )
        .withColumn(
            "lng",
            col("fixed_lng").when(col("fixed_lng").isNotNull(), col("fixed_lng")).otherwise(col("lng"))
        )
        .drop("fixed_lat", "fixed_lng")
    )

    fixed_df = spark.createDataFrame(fixed_rows, schema)

    df_enriched.filter(
        (col("lat").isNull()) |
        (col("lng").isNull()) |
        (col("lat") == 0) |
        (col("lng") == 0)
    ).count()

    spark.stop()

if __name__ == "__main__":
    main()