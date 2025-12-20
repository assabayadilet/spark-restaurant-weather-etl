# spark-docker

Apache Spark 3.5.3 standalone cluster deployed with Docker Compose.  
The project demonstrates a step-by-step Spark ETL pipeline using a local file system as a data lake (RAW → PROCESSED).

---

## Prerequisites

- Docker & Docker Compose
- Git
- Local clone of this repository
- Dataset from **Spark Practice — Task**
  - `restaurant_csv` (CSV files)
  - `weather` (partitioned parquet files, used later)
- (Planned) OpenCage Geocoding API key

---

## Project structure

```text
.
├── docker-compose.yml
├── Dockerfile
├── start-spark.sh
├── jobs
│   ├── raw
│   │   ├── restaurants_raw.py
│   │   └── weather_raw.py          # planned
│   ├── restaurants_etl.py          # planned
│   ├── weather_etl.py              # planned
│   └── restaurants_weather_join.py # planned
├── data                             # mounted volume (ignored by git)
│   ├── restaurant_csv
│   ├── weather
│   └── raw
│       └── restaurants
└── README.md