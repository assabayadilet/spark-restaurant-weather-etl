from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os

# 0) CONFIG — paths MUST match current project structure

BASE_PATH = "/app/data"

RESTAURANTS_PATH = f"{BASE_PATH}/input/restaurant_csv"
WEATHER_PATH = f"{BASE_PATH}/input/Weather"   # IMPORTANT: folder name is case-sensitive


# ======================================================
# 1) CREATE SPARK SESSION
# ======================================================
def create_spark(app_name: str) -> SparkSession:
    """
    SparkSession for local development (IDE / Docker).
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ======================================================
# 2) READ INPUT DATASETS
# ======================================================
def read_restaurants(spark: SparkSession) -> DataFrame:
    """
    Step 1 (Task):
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
    base = "/app/data/input/weather"   # или "/data/input/weather" если у вас другой монт
    parquet_files = []
    # Resolve actual on-disk path (container may mount project under /app or /data)
    candidates = [
        os.path.join("/app", WEATHER_PATH),
        os.path.join("/data", WEATHER_PATH),
        os.path.join("/app", BASE_PATH, "input", "Weather"),
        os.path.join(BASE_PATH, "input", "Weather"),
        WEATHER_PATH,
    ]
    actual = next((p for p in candidates if os.path.exists(p)), None)
    if actual is None:
        raise FileNotFoundError(f"Weather path not found, tried: {candidates}")

    # If directory contains multiple differently-named subfolders (e.g. 'weather', 'weather 5', ...),
    # load each subfolder separately and union them to avoid Spark partition inference conflicts.
    subdirs = [os.path.join(actual, d) for d in os.listdir(actual) if os.path.isdir(os.path.join(actual, d))]
    parquet_dirs = []
    for d in subdirs:
        # check if this subdir contains parquet files (or nested partition dirs)
        contains_parquet = False
        for root, _, files in os.walk(d):
            if any(f.endswith(".parquet") or f.endswith(".snappy.parquet") for f in files):
                contains_parquet = True
                break
        if contains_parquet:
            parquet_dirs.append(d)

    # If no subdirs with parquet found, maybe parquet files are directly under actual
    if not parquet_dirs:
        return spark.read.parquet(actual)

    # Load each parquet directory and union them
    df = None
    for p in parquet_dirs:
        part = spark.read.parquet(p)
        df = part if df is None else df.unionByName(part)
    return df
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {base}")
    return spark.read.parquet(*parquet_files)


# ======================================================
# 3) INSPECTION / DEBUG HELPERS
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
# 4) MAIN — PIPELINE ENTRY POINT
# ======================================================
def main() -> None:
    spark = create_spark("Spark Practice | Read & Inspect")

    # ---- Step A: Read datasets
    restaurants_df = read_restaurants(spark)
    weather_df = read_weather(spark)

    # ---- Step B: Inspect structure and size
    inspect_df(restaurants_df, "Restaurants")
    inspect_df(weather_df, "Weather")

    spark.stop()


# ======================================================
# 5) SCRIPT ENTRY
# ======================================================
if __name__ == "__main__":
    main()