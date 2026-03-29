# Spark ETL — Restaurant & Weather Data Enrichment

A **PySpark ETL pipeline** that enriches restaurant data with weather information using geohash-based spatial joins. Containerized with Docker, tested with pytest.

## Architecture

```
┌─────────────────┐   ┌─────────────────┐
│  Restaurants     │   │    Weather       │
│  (CSV)           │   │  (Parquet)       │
└────────┬────────┘   └────────┬─────────┘
         │                     │
         ▼                     ▼
┌─────────────────────────────────────────┐
│           PySpark Pipeline              │
│                                         │
│  1. Validate coordinates                │
│  2. Geocode missing via OpenCage API    │
│  3. Generate 4-char geohash            │
│  4. Left join on geohash               │
│  5. Write partitioned Parquet          │
└─────────────────────┬───────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Enriched Parquet      │
         │  partitioned by        │
         │  country / city        │
         └────────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Processing | Apache Spark (PySpark) |
| Geocoding | OpenCage API (REST) |
| Geospatial | geohash2 library |
| Infrastructure | Docker + Docker Compose |
| Testing | pytest (with mocked API calls) |

## Project Structure

```
├── jobs/
│   └── main.py                  # Main Spark ETL job
├── tests/
│   └── test_main.py             # Unit tests
├── data/
│   ├── input/
│   │   ├── restaurant_csv/      # Restaurant data
│   │   └── weather/             # Weather data (partitioned Parquet)
│   └── output/
│       └── enriched_parquet/    # Output (created by job)
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Quick Start

```bash
# Start Spark cluster
docker-compose up -d

# Run ETL job
docker-compose exec spark-master bash -c \
  "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/jobs/main.py"
```

## Configuration

Set your OpenCage API key in `docker-compose.yml`:

```yaml
services:
  spark-master:
    environment:
      - OPENCAGE_API_KEY=your_key_here
```

## Pipeline Steps

| Step | Description |
|------|-------------|
| 1. Validate | Split restaurants into valid/invalid coordinates |
| 2. Geocode | Call OpenCage API for missing lat/lng |
| 3. Geohash | Generate 4-character geohash for spatial matching |
| 4. Join | Left join restaurants ⟕ weather on geohash |
| 5. Write | Output as partitioned Parquet (country/city) |

## Tests

```bash
docker-compose exec spark-master bash -c "cd /app && python -m pytest -q tests/test_main.py"
```

Tests cover:
- Geohash generation (deterministic, handles nulls)
- Coordinate validation (valid/invalid split)
- Geohash enrichment (column addition, matching)
- Mocked API calls (no real HTTP in tests)

## Key Design Decisions

- **Geohash precision 4**: Balances spatial accuracy with join granularity
- **Left join**: Preserves all restaurants even without weather match
- **Overwrite mode**: Ensures idempotent re-runs
- **Mocked geocoding in tests**: No external dependencies for CI
