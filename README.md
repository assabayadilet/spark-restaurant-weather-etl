# spark-docker
Spark standalone cluster with docker compose

## Prerequisites
- Install Apache Spark locally (native install per the [Spark documentation](https://spark.apache.org/docs/latest/) or via Docker using this repository). Development and testing should run in your local IDE environment.
- Download the **Spark Practice—Dataset** from the task description page and place it in local storage accessible to your Spark job.
- Provision API credentials for the [OpenCage Geocoding API](https://opencagedata.com/api) so the ETL job can enrich invalid coordinates.
- Add a geohash library dependency (e.g., [geohash-java](https://github.com/kungfoo/geohash)) to your project so you can derive four-character hashes.
- Prepare local directories for weather data, restaurant data, and the enriched parquet output so the job can preserve partitioning when writing to disk.

## Task requirements
1. **Read source data** – Create a Spark ETL job that loads restaurant data from the downloaded dataset in local storage.
2. **Validate coordinates** – Detect records with missing or null latitude/longitude values. For each invalid record, query the OpenCage Geocoding REST API to retrieve the correct coordinates and update the record within the job.
3. **Compute geohash** – Use the geohash library to calculate a four-character geohash for every restaurant row and store it in a new column.
4. **Join with weather data** – Load the local weather dataset, left-join it with the restaurant data on the four-character geohash, and ensure the transformation is idempotent (no duplicated records when the job is re-run).
5. **Write enriched output** – Persist the fully joined dataset (all columns from restaurants and weather) to the local file system as partitioned parquet files while preserving the original partitioning scheme.
6. **Testing & documentation** – Add automated tests covering the ETL logic where practical, then document the full workflow in this README with run instructions, screenshots, and commentary before sharing the repository link.

## Expected deliverables
- Source code for the ETL job plus supporting modules and tests.
- Evidence of local execution (screenshots, logs, and narrative) embedded in this README.
- Instructions for building/running the job, including how to configure Spark, API keys, dataset paths, and output locations.

---

## Cluster quickstart

1. Build locally the docker image
```
docker build -t cluster-apache-spark:3.5.3 .
```

2. Deploy the Spark Standalone cluster
```
docker-compose up -d
```

3. Verify spark cluster is up and running

### List docker running containers
```
docker ps -a
```

You should get a similar output like this:
```
CONTAINER ID   IMAGE                        COMMAND                  CREATED          STATUS          PORTS                                                      NAMES
6bddbd3b5cfa   cluster-apache-spark:3.5.3   "/bin/bash /start-sp…"   11 minutes ago   Up 11 minutes   7077/tcp, 0.0.0.0:7001->7000/tcp, 0.0.0.0:9091->8080/tcp   spark-docker-spark-worker-a-1
4195b53e7543   cluster-apache-spark:3.5.3   "/bin/bash /start-sp…"   15 minutes ago   Up 15 minutes   7000/tcp, 0.0.0.0:7077->7077/tcp, 0.0.0.0:9090->8080/tcp   spark-docker-spark-master-1
```

### Container exposed ports

container|Exposed ports
---|---
spark-master|9090 7077
spark-worker-1|9091

#### Spark Master
http://localhost:9090/

#### Spark Worker 1
http://localhost:9091/

### Create a Spark Session from python using PySpark

Create a `.py` file with the following content and run it:

```python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .master("spark://localhost:7077") \
    .getOrCreate()
```

```python
# Print Spark session details
print("Spark Application Name:", spark.sparkContext.appName)
print("Spark Master URL:", spark.sparkContext.master)
print("Spark Version:", spark.version)
print("Spark Application ID:", spark.sparkContext.applicationId)
print("Spark Web UI URL:", spark.sparkContext.uiWebUrl)
print("Spark User:", spark.sparkContext.sparkUser())
print("Spark Configurations:")
for key, value in spark.sparkContext.getConf().getAll():
    print(f"  {key}: {value}")
```

## Run the restaurant + weather ETL job

1. Rebuild the Docker image to pick up the job dependencies:
   ```
   docker build -t cluster-apache-spark:3.5.3 .
   ```
2. Place the CSV datasets in the shared `data` directory (e.g., `data/input/restaurants.csv` and `data/input/weather.csv`). The folder is mounted into each container at `/opt/spark-data`.
3. Export your OpenCage key so the job can enrich missing coordinates:
   ```
   export OPENCAGE_API_KEY=<your-key>
   ```
4. Submit the PySpark job from the master container:
   ```
   docker exec -it spark-docker-spark-master-1 /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /opt/spark-jobs/restaurant_weather_etl.py \
     --restaurants-path /opt/spark-data/input/restaurants.csv \
     --weather-path /opt/spark-data/input/weather.csv \
     --output-path /opt/spark-data/output/enriched_restaurants \
     --address-columns "address,city,country" \
     --partition-columns "country"
   ```
   Adjust the column arguments if your CSV headers differ. By default the job:
   - Detects null/NaN latitude or longitude, queries OpenCage via REST, and replaces the coordinates.
   - Computes a four-character geohash for restaurants and weather data.
   - Drops duplicate weather rows per geohash, left-joins the datasets, and writes partitioned parquet output (idempotent `overwrite` mode) to the specified folder.
5. Inspect the parquet files inside `data/output/enriched_restaurants` or attach them in your homework submission together with screenshots/logs from the Spark UI.
