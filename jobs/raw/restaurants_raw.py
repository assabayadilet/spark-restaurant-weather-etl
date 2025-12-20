from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("Restaurants RAW")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )   
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/data/restaurant_csv")
    )

    df.write.mode("overwrite").parquet("/data/raw/restaurants")

    print(f"RAW rows: {df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()