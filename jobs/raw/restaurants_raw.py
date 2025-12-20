from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("Restaurants RAW - Read Only")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    BASE_PATH = "/opt/spark-data"
    input_path = f"{BASE_PATH}/input/restaurant_csv"
    output_path = f"{BASE_PATH}/raw/restaurants"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print("=== Restaurants schema ===")
    df.printSchema()

    print("=== Sample rows ===")
    df.show(5, truncate=False)

    print(f"Total rows: {df.count()}")

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()