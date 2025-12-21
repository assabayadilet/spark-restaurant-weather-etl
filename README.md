# Spark Practice – Restaurants & Weather Enrichment

This project is a small Spark ETL job that enriches restaurant data with weather information.

The pipeline:

1. Reads **restaurants** (CSV) and **weather** (Parquet, partitioned) from local storage.
2. Checks restaurant coordinates for incorrect / missing values (`lat`, `lng`).
3. For incorrect values, calls the **OpenCage Geocoding API** (REST) to map `lat`/`lng` from `city` + `country`.
4. Generates a **4-character geohash** for both restaurants and weather.
5. Left-joins restaurants with weather on geohash.
6. Stores enriched data in the local file system as **partitioned Parquet**.
7. Core logic is covered by unit tests using **pytest**.

Development and testing are done locally via Docker + PySpark + pytest.

---

## 1. Data

### 1.1. Restaurants (CSV)

**Input path inside the container**

```text
/app/data/input/restaurant_csv
```

**Schema**

```text
root
 |-- id: long (nullable = true)
 |-- franchise_id: integer (nullable = true)
 |-- franchise_name: string (nullable = true)
 |-- restaurant_franchise_id: integer (nullable = true)
 |-- country: string (nullable = true)
 |-- city: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- lng: double (nullable = true)
```

### 1.2. Weather (Parquet, partitioned)

**Input path inside the container**  
(directory with multiple subfolders, each containing Parquet files):

```text
/app/data/input/weather
```

**Schema**

```text
root
 |-- lng: double (nullable = true)
 |-- lat: double (nullable = true)
 |-- avg_tmpr_f: double (nullable = true)
 |-- avg_tmpr_c: double (nullable = true)
 |-- wthr_date: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- day: integer (nullable = true)
```

The weather dataset is partitioned by `year`, `month`, `day`.

---

## 2. Project structure

Simplified structure relevant to this homework:

```text
spark-docker/
  docker-compose.yml

  jobs/
    __init__.py
    main.py              # main Spark ETL job

  tests/
    test_main.py         # unit tests for core logic

  data/
    input/
      restaurant_csv/    # restaurant CSV files
      weather/           # weather parquet folders (weather, weather 2, weather 3, ...)
    output/
      enriched_parquet/  # enriched data in parquet (created by the job)
```

---

## 3. How to run the Spark job

### 3.1. Start the cluster

From the project root:

```bash
docker-compose up -d
```

This starts the Spark master and workers.

### 3.2. Submit the job

```bash
docker-compose exec spark-master bash -c \
  "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/jobs/main.py"
```

In the logs you will see:

- schemas for Restaurants and Weather;
- row counts for both datasets;
- statistics about valid / invalid coordinates;
- example of geocoding from OpenCage for one invalid restaurant;
- sample rows with generated geohash;
- information about written Parquet output.

---

## 4. Configuration

### 4.1. Base paths

In `jobs/main.py`:

```python
BASE_PATH = "/app/data"

RESTAURANTS_PATH = f"{BASE_PATH}/input/restaurant_csv"
WEATHER_PATH     = f"{BASE_PATH}/input/weather"
```

These paths must match the volume mounts in `docker-compose.yml`.

### 4.2. OpenCage Geocoding API

For invalid restaurant coordinates the job calls:

```text
GET https://api.opencagedata.com/geocode/v1/json
```

The API key is taken from the environment:

```bash
OPENCAGE_API_KEY=<your_api_key>
```

Example configuration in `docker-compose.yml`:

```yaml
services:
  spark-master:
    environment:
      - OPENCAGE_API_KEY=YOUR_KEY_HERE
```

In the code there is a helper function (conceptually):

```python
def geocode_with_opencage(city: str, country: str) -> tuple[float | None, float | None]:
    """Calls OpenCage API and returns (lat, lng) for given city + country.
    Returns (None, None) if nothing is found.
    """
```

---

## 5. Pipeline logic (tasks)

### Task 1 – Validate & inspect coordinates

**Requirement**

> Check restaurant data for incorrect (null) values (latitude and longitude).  
> For incorrect values, map latitude and longitude from the OpenCage Geocoding API in a job via the REST API.

**Implementation**

- Split restaurants into:
  - **valid**: `lat IS NOT NULL AND lng IS NOT NULL`
  - **invalid**: `lat IS NULL OR lng IS NULL`
- Log:
  - valid restaurants count
  - invalid restaurants count
  - sample invalid rows
- Demonstrate a call to OpenCage for at least one invalid row (take `city`, `country`, call API, print returned `lat`, `lng`).
- In the main pipeline we proceed with **valid** restaurants (non-null coordinates).

### Task 2 – Generate geohash

**Requirement**

> Generate a geohash by latitude and longitude using a geohash library like geohash-java.  
> Your geohash should be four characters long and placed in an extra column.

**Implementation**

Use Python library `geohash2` and define:

```python
import geohash2


def geohash_4(lat: float, lng: float) -> str | None:
    if lat is None or lng is None:
        return None
    return geohash2.encode(lat, lng, precision=4)
```

Register it as a Spark UDF and add `geohash` column:

- for restaurants – based on restaurant `lat`/`lng`;
- for weather – based on weather `lat`/`lng`.

Log small samples of both DataFrames with `geohash` for validation.

### Task 3 – Join restaurants with weather

**Requirement**

> Left-join weather and restaurant data using the four-character geohash.  
> Make sure to avoid data multiplication and keep your job idempotent.

**Implementation**

```python
enriched_df = restaurants_geo_df.join(
    weather_geo_df,
    on="geohash",
    how="left",
)
```

- All restaurants remain in the result.
- Weather columns are added where geohash matches.
- If there is no weather for some restaurant geohash, restaurant still exists with `NULL` weather fields.

Idempotency:

- no state is kept between runs;
- writing to output uses `.mode("overwrite")`, so the job can be re-run safely.

### Task 4 – Save enriched data as partitioned Parquet

**Requirement**

> Store the enriched data (i.e., the joined data with all the fields from both datasets) in the local file system, preserving data partitioning in the parquet format.

**Implementation**

Output path inside container:

```python
OUTPUT_PATH = f"{BASE_PATH}/output/enriched_parquet"
```

Write as Parquet, partitioned by `year`, `month`, `day` (fields coming from the weather dataset):

```python
(
    enriched_df
    .write
    .mode("overwrite")           # idempotent
    .partitionBy("year", "month", "day")
    .parquet(OUTPUT_PATH)
)
```

This preserves the logical partitioning of the weather data in the final enriched dataset.

---

## 6. Main pipeline (`main()`)

High-level orchestration in `jobs/main.py`:

1. Create Spark session.
2. Read restaurants CSV.
3. Read weather Parquet (multiple subfolders under `/app/data/input/weather`).
4. Inspect both DataFrames (schema, count, sample rows).
5. **Task 1:** process restaurant coordinates (split valid/invalid, log stats, demo OpenCage call, return valid restaurants).
6. **Task 2:** add geohash columns to restaurants and weather.
7. **Task 3:** left-join restaurants with weather on geohash.
8. **Task 4:** write enriched data as partitioned Parquet to `/app/data/output/enriched_parquet`.
9. Stop Spark session.

---

## 7. Unit tests

Unit tests are implemented with `pytest` in `tests/test_main.py` and run inside the `spark-master` container.

### 7.1. What is tested

- `geohash_4`:
  - returns a 4-character string;
  - deterministic for the same coordinates;
  - safely handles `None` inputs.
- Coordinate processing logic:
  - correctly splits restaurants into valid / invalid by `lat` / `lng`;
  - uses a mocked version of `geocode_with_opencage` (no real HTTP calls).
- Geohash enrichment:
  - adds `geohash` column to both restaurants and weather DataFrames;
  - `geohash` matches for the same pair of coordinates.

### 7.2. How to run tests

From project root:

```bash
docker-compose exec spark-master bash -c "cd /app && python -m pytest -q tests/test_main.py"
```

Example expected output:

```text
....                                                                                         [100%]
4 passed in 6.92s
```

---

## 8. Summary

This homework demonstrates:

- Reading heterogeneous data sources (CSV + partitioned Parquet) in Spark.
- Performing basic data quality checks on coordinates.
- Calling an external REST API (OpenCage) from a Spark job.
- Computing and using geohash for spatial joins.
- Writing idempotent, partitioned Parquet output.
- Covering core transformation logic with unit tests (`pytest`) inside a Dockerized Spark environment.