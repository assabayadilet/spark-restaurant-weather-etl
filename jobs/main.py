from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
import requests
from pyspark.sql.functions import col
import geohash2
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 0) CONFIG — paths MUST match current project structure

BASE_PATH = "/app/data"

RESTAURANTS_PATH = f"{BASE_PATH}/input/restaurant_csv"
WEATHER_PATH = f"{BASE_PATH}/input/Weather"

def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_restaurants(spark: SparkSession) -> DataFrame:
    """
    Read restaurants data from CSV files.
    Spark reads ALL csv files inside the folder.
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RESTAURANTS_PATH)
    )

def read_weather(spark: SparkSession) -> DataFrame:
    """
    Read weather data from multiple parquet datasets located under one directory.
    Each subdirectory (e.g. weather, weather 2, ...) is treated as a separate
    parquet root and unioned to avoid Spark partition conflicts.
    """

    actual = os.path.join("/app", WEATHER_PATH)
    if not os.path.exists(actual):
        raise FileNotFoundError(f"Weather path not found: {actual}")

    # list only first-level subdirectories (weather, weather 2, ...)
    parquet_roots = [
        os.path.join(actual, d)
        for d in os.listdir(actual)
        if os.path.isdir(os.path.join(actual, d))
    ]

    if not parquet_roots:
        raise RuntimeError(f"No weather parquet datasets found in {actual}")

    df = None
    for root in parquet_roots:
        part = spark.read.parquet(root)
        df = part if df is None else df.unionByName(part)

    return df

# ======================================================
# INSPECTION / DEBUG HELPERS
# ======================================================
def inspect_df(df: DataFrame, name: str) -> None:
    """
    Print schema, row count and sample rows.
    Used for validation and README screenshots.
    """
    print(f"\n================ {name} =================")
    print("Schema:")
    df.printSchema()

    print("Total records:")
    print(df.count())

    print("Sample rows:")
    df.show(5, truncate=False)

# ======================================================
# TASK 1.1: Check restaurant data for incorrect coordinates
# ======================================================

def geocode_with_opencage(city: str, country: str):

    api_key = "0285d06047d84d0ab21b0e2e82078e59"

    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {
        "q": f"{city}, {country}",
        "key": api_key,
        "limit": 1
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    results = response.json().get("results")
    if not results:
        return None, None

    geometry = results[0]["geometry"]
    return geometry["lat"], geometry["lng"]    

def process_restaurant_coordinates(restaurants_df: DataFrame) -> DataFrame:
    """
    TASK 1:
    - Validate restaurant coordinates
    - Geocode invalid records using OpenCage API
    """

    invalid_df = restaurants_df.filter(col("lat").isNull() | col("lng").isNull())
    valid_df = restaurants_df.subtract(invalid_df)

    print("\n================ TASK 1.1 RESULTS ================")
    print(f"Valid restaurants count: {valid_df.count()}")
    print(f"Invalid restaurants count: {invalid_df.count()}")
    invalid_df.show(5, truncate=False)

    # Geocode first invalid record (demo purpose)
    row = invalid_df.collect()[0]
    lat, lng = geocode_with_opencage(row["city"], row["country"])

    print("\n================ TASK 1.2 RESULTS ================")
    print(f"Geocoded location: {row['city']}, {row['country']}")
    print(f"Latitude: {lat}, Longitude: {lng}")

    return valid_df

def geohash_4(lat: float, lng: float) -> str:
    """
    Generate a 4-character geohash from latitude and longitude.
    """
    if lat is None or lng is None:
        return None
    return geohash2.encode(lat, lng, precision=4)

geohash_udf = udf(geohash_4, StringType())

def add_geohash_columns(
    restaurants_df: DataFrame,
    weather_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    TASK 2:
    - Generate 4-character geohash for restaurants and weather
    - Add it as a new column 'geohash'
    - Print small samples for README / validation
    """

    restaurants_geo_df = restaurants_df.withColumn(
        "geohash", geohash_udf(col("lat"), col("lng"))
    )

    weather_geo_df = weather_df.withColumn(
        "geohash", geohash_udf(col("lat"), col("lng"))
    )

    print("\n================ TASK 2 RESULTS (Restaurants) ================")
    restaurants_geo_df.select(
        "id", "city", "country", "lat", "lng", "geohash"
    ).show(5, truncate=False)

    print("\n================ TASK 2 RESULTS (Weather) ================")
    weather_geo_df.select(
        "lat", "lng", "geohash", "year", "month", "day"
    ).show(5, truncate=False)

    return restaurants_geo_df, weather_geo_df

def join_restaurants_with_weather(
    restaurants_geo_df: DataFrame,
    weather_geo_df: DataFrame,
) -> DataFrame:
    """\
    TASK 3:
    - Left-join restaurants with weather on 4-character geohash
    - Avoid data multiplication by deduplicating weather by (geohash, year, month, day)
    - Return enriched dataframe with all restaurant fields + weather fields
    """

    # Deduplicate weather per geohash & date to reduce data multiplication risk
    weather_dedup = weather_geo_df.dropDuplicates(["geohash", "year", "month", "day"])
    weather_dedup = weather_dedup.drop("lat", "lng")

    enriched_df = restaurants_geo_df.join(
        weather_dedup,
        on="geohash",
        how="left",
    )

    print("\n================ TASK 3 RESULTS (Join) ================")
    enriched_df.select(
        "id",
        "city",
        "country",
        "lat",
        "lng",
        "geohash",
        "year",
        "month",
        "day",
        "avg_tmpr_c",
        "avg_tmpr_f",
    ).show(5, truncate=False)

    return enriched_df

def write_enriched_data(enriched_df: DataFrame) -> None:
    """\
    TASK 4:
    - Write enriched data to local filesystem in Parquet format
    - Preserve partitioning by year, month, day (from weather data)
    - Use overwrite mode to keep the job idempotent
    """

    output_path = f"{BASE_PATH}/output/enriched_parquet"

    (
        enriched_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )

    print("\n================ TASK 4 RESULTS (Write) ================")
    print(f"Enriched data written to: {output_path}")

# ======================================================
# 4) MAIN — PIPELINE ENTRY POINT
# ======================================================
def main() -> None:
    spark = create_spark("Spark Practice | Read & Inspect")

    # ---- Read datasets
    restaurants_df = read_restaurants(spark)
    weather_df = read_weather(spark)

    # ---- Inspect structure and size
    inspect_df(restaurants_df, "Restaurants")
    inspect_df(weather_df, "Weather")

    # ---- TASK 1: Process restaurant coordinates
    valid_restaurants_df = process_restaurant_coordinates(restaurants_df)

    # ---- TASK 2: Generate geohash columns
    restaurants_geo_df, weather_geo_df = add_geohash_columns(
        valid_restaurants_df,
        weather_df,
    )

    # ---- TASK 3: Join restaurants with weather
    enriched_df = join_restaurants_with_weather(restaurants_geo_df, weather_geo_df)

    # ---- TASK 4: Write enriched data as partitioned Parquet
    write_enriched_data(enriched_df)

    #---- Read enriched data back for inspection
    enriched_readback_df = spark.read.parquet(f"{BASE_PATH}/output/enriched_parquet")
    inspect_df(enriched_readback_df, "Enriched Restaurants with Weather")


    spark.stop()


# ======================================================
# 5) SCRIPT ENTRY
# ======================================================
if __name__ == "__main__":
    main()