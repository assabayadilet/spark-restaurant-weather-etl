import argparse
import logging
import os
import sys
import time
from typing import Iterable, List, Optional, Sequence, Tuple

import requests
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

try:
	from geohash2 import encode as geohash_encode
except ImportError:  # pragma: no cover - handled at runtime
	geohash_encode = None


LOGGER = logging.getLogger("restaurant_weather_etl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OPEN_CAGE_URL = "https://api.opencagedata.com/geocode/v1/json"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Restaurant + weather enrichment ETL job.")
	parser.add_argument("--master-url", default="spark://spark-master:7077", help="Spark master URL to connect to.")
	parser.add_argument("--restaurants-path", required=True, help="Path to the restaurant CSV dataset in local storage.")
	parser.add_argument("--weather-path", required=True, help="Path to the weather CSV dataset in local storage.")
	parser.add_argument("--output-path", required=True, help="Target directory for the enriched parquet output.")
	parser.add_argument("--address-columns", default="address,city,country",
	                    help="Comma-separated list of columns to build the geocoding search query.")
	parser.add_argument("--geohash-precision", type=int, default=4, help="Geohash length (4 required by the task).")
	parser.add_argument("--opencage-rate-limit-ms", type=int, default=1200,
	                    help="Sleep duration in milliseconds between OpenCage requests to avoid throttling.")
	parser.add_argument("--opencage-api-key", default=os.getenv("OPENCAGE_API_KEY"),
	                    help="OpenCage API key (falls back to OPENCAGE_API_KEY env variable).")
	parser.add_argument("--partition-columns", default="",
	                    help="Comma-separated list of columns to partition the parquet output.")
	parser.add_argument("--weather-geohash-columns", default="latitude,longitude",
	                    help="Comma-separated lat/long columns inside the weather dataset.")
	parser.add_argument("--restaurants-geohash-columns", default="latitude,longitude",
	                    help="Comma-separated lat/long columns inside the restaurant dataset.")
	return parser.parse_args(argv)


def build_geohash_udf(precision: int) -> F.udf:
	if not geohash_encode:
		raise RuntimeError("geohash2 module is not available. Install it or rebuild the Docker image.")

	def _encode(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
		if lat is None or lon is None:
			return None
		return geohash_encode(lat, lon, precision=precision)

	return F.udf(_encode, T.StringType())


def build_geocode_udf(
	address_columns: List[str],
	api_key: str,
	sleep_ms: int,
) -> F.udf:
	geocode_schema = T.StructType([
	    T.StructField("latitude", T.DoubleType()),
	    T.StructField("longitude", T.DoubleType()),
	])

	def _geocode(*cols: Iterable[Optional[str]]) -> Tuple[Optional[float], Optional[float]]:
		query = ", ".join([str(value).strip() for value in cols if value and str(value).strip()])
		if not query:
			return None, None

		try:
			response = requests.get(
			    OPEN_CAGE_URL,
			    params={
			        "q": query,
			        "key": api_key,
			        "limit": 1,
			        "no_annotations": 1,
			    },
			    timeout=15,
			)
			if response.status_code == 200:
				payload = response.json()
				results = payload.get("results", [])
				if results:
					geometry = results[0].get("geometry", {})
					return geometry.get("lat"), geometry.get("lng")
			else:
				LOGGER.warning("OpenCage request failed for query '%s' (status=%s)", query, response.status_code)
		except Exception as exc:  # pylint: disable=broad-except
			LOGGER.warning("OpenCage lookup error for query '%s': %s", query, exc)
		finally:
			# OpenCage free tier allows 1 request/s, so respect the limit.
			time.sleep(max(sleep_ms, 0) / 1000.0)
		return None, None

	return F.udf(_geocode, geocode_schema)


def coalesce_partitions(df: DataFrame, partition_cols: List[str]) -> DataFrame:
	if partition_cols:
		return df
	# Avoid producing a single small parquet file; let Spark decide if not partitioning.
	return df.coalesce(1)


def main(argv: Optional[Sequence[str]] = None) -> None:
	args = parse_args(argv)
	address_columns = [col.strip() for col in args.address_columns.split(",") if col.strip()]
	if not address_columns:
		raise ValueError("At least one address column must be provided for geocoding.")

	restaurant_lat_col, restaurant_lon_col = [c.strip() for c in args.restaurants_geohash_columns.split(",")]
	weather_lat_col, weather_lon_col = [c.strip() for c in args.weather_geohash_columns.split(",")]

	spark = SparkSession.builder.appName("RestaurantWeatherETL").master(args.master_url).getOrCreate()

	LOGGER.info("Reading restaurant dataset from %s", args.restaurants_path)
	restaurants_df = spark.read.option("header", True).option("inferSchema", True).csv(args.restaurants_path)

	LOGGER.info("Reading weather dataset from %s", args.weather_path)
	weather_df = spark.read.option("header", True).option("inferSchema", True).csv(args.weather_path)

	def _invalid(col_name: str) -> F.Column:
		return F.col(col_name).isNull() | F.isnan(F.col(col_name))

	invalid_condition = _invalid(restaurant_lat_col) | _invalid(restaurant_lon_col)
	invalid_count = restaurants_df.filter(invalid_condition).count()
	LOGGER.info("Restaurant rows requiring geocoding: %s", invalid_count)

	if invalid_count > 0 and not args.opencage_api_key:
		raise RuntimeError(
		    "Null latitude/longitude detected but OPENCAGE_API_KEY is missing. "
		    "Set --opencage-api-key or export OPENCAGE_API_KEY."
		)

	if invalid_count > 0:
		geocode_udf = build_geocode_udf(address_columns, args.opencage_api_key, args.opencage_rate_limit_ms)
		restaurants_df = restaurants_df.withColumn(
		    "resolved_coordinates",
		    F.when(
		        invalid_condition,
		        geocode_udf(*[F.col(col) for col in address_columns])
		    ).otherwise(F.struct(F.col(restaurant_lat_col).alias("latitude"),
		                         F.col(restaurant_lon_col).alias("longitude")))
		).withColumn(
		    restaurant_lat_col,
		    F.coalesce(F.col("resolved_coordinates.latitude"), F.col(restaurant_lat_col))
		).withColumn(
		    restaurant_lon_col,
		    F.coalesce(F.col("resolved_coordinates.longitude"), F.col(restaurant_lon_col))
		).drop("resolved_coordinates")

	geohash_udf = build_geohash_udf(args.geohash_precision)

	restaurants_df = restaurants_df.withColumn(
	    "geohash",
	    geohash_udf(F.col(restaurant_lat_col), F.col(restaurant_lon_col))
	)

	weather_df = weather_df.withColumn(
	    "geohash",
	    geohash_udf(F.col(weather_lat_col), F.col(weather_lon_col))
	).dropDuplicates(["geohash"])

	enriched_df = restaurants_df.join(weather_df, on="geohash", how="left")

	partition_cols = [col.strip() for col in args.partition_columns.split(",") if col.strip()]
	final_df = coalesce_partitions(enriched_df, partition_cols)

	writer = final_df.write.mode("overwrite").format("parquet")
	if partition_cols:
		writer = writer.partitionBy(*partition_cols)

	LOGGER.info("Writing enriched data to %s", args.output_path)
	writer.save(args.output_path)

	LOGGER.info("ETL job finished successfully.")
	spark.stop()


if __name__ == "__main__":  # pragma: no cover
	try:
		main(sys.argv[1:])
	except Exception as exc:  # pylint: disable=broad-except
		LOGGER.error("ETL job failed: %s", exc)
		raise
