import pytest
from pyspark.sql import SparkSession, Row

import jobs.main as main_mod  # import the module as a whole instead of individual functions


# ---------- Spark fixture ----------

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("spark-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------- TASK 2: geohash_4 ----------

def test_geohash_4_basic():
    lat, lng = 48.8566, 2.3522  # Paris
    gh = main_mod.geohash_4(lat, lng)

    assert isinstance(gh, str)
    assert len(gh) == 4
    # determinism: repeated calls with same inputs must return the same value
    assert main_mod.geohash_4(lat, lng) == gh


def test_geohash_4_none_safe():
    gh = main_mod.geohash_4(None, 10.0)
    assert gh is None
    gh = main_mod.geohash_4(10.0, None)
    assert gh is None


# ---------- TASK 1: process_restaurant_coordinates ----------

def test_process_restaurant_coordinates_split_and_geocode(spark, monkeypatch):
    data = [
        Row(id=1, city="Paris",  country="FR", lat=48.8566, lng=2.3522),
        Row(id=2, city="Dillon", country="US", lat=None,     lng=-79.38),
        Row(id=3, city="Dillon", country="US", lat=34.40,    lng=None),
    ]
    df = spark.createDataFrame(data)

    def fake_geocode(city, country):
        return 1.23, 4.56

    monkeypatch.setattr(main_mod, "geocode_with_opencage", fake_geocode)

    valid_df = main_mod.process_restaurant_coordinates(df)

    valid_ids = {row.id for row in valid_df.collect()}
    assert valid_ids == {1}


# ---------- TASK 2: add_geohash_columns ----------

def test_add_geohash_columns_adds_geohash(spark):
    restaurants = spark.createDataFrame(
        [Row(id=1, city="Paris", country="FR", lat=48.8566, lng=2.3522)]
    )

    weather = spark.createDataFrame(
        [Row(lat=48.8566, lng=2.3522, year=2020, month=1, day=1)]
    )

    restaurants_geo_df, weather_geo_df = main_mod.add_geohash_columns(
        restaurants,
        weather,
    )

    # Check that the geohash column is added to both dataframes
    assert "geohash" in restaurants_geo_df.columns
    assert "geohash" in weather_geo_df.columns

    r_row = restaurants_geo_df.select("geohash").first()
    w_row = weather_geo_df.select("geohash").first()

    # geohash is not null and matches in both dataframes
    assert r_row.geohash is not None
    assert w_row.geohash is not None
    assert len(r_row.geohash) == 4
    assert r_row.geohash == w_row.geohash


# ---------- TASK 3: join_restaurants_with_weather ----------

def test_join_restaurants_with_weather_left_join(spark):
    """Restaurants must be preserved (left join) and weather columns attached when geohash matches."""
    # restaurant with a predefined geohash
    restaurants_geo_df = spark.createDataFrame(
        [
            Row(
                id=1,
                city="Paris",
                country="FR",
                lat=48.8566,
                lng=2.3522,
                geohash="u09t",
            )
        ]
    )

    # weather with the same geohash and date partitions
    weather_geo_df = spark.createDataFrame(
        [
            Row(
                geohash="u09t",
                lat=48.8566,
                lng=2.3522,
                avg_tmpr_c=25.0,
                avg_tmpr_f=77.0,
                wthr_date="2020-01-01",
                year=2020,
                month=1,
                day=1,
            )
        ]
    )

    enriched_df = main_mod.join_restaurants_with_weather(
        restaurants_geo_df,
        weather_geo_df,
    )

    rows = enriched_df.collect()
    assert len(rows) == 1

    row = rows[0]
    # restaurant fields are preserved
    assert row.id == 1
    assert row.city == "Paris"
    # weather fields are correctly joined
    assert row.avg_tmpr_c == 25.0
    assert row.avg_tmpr_f == 77.0
    assert row.year == 2020
    assert row.month == 1
    assert row.day == 1


def test_join_restaurants_with_weather_preserves_all_restaurants(spark):
    """Left join must keep restaurants without matching weather as well."""
    restaurants_geo_df = spark.createDataFrame(
        [
            Row(id=1, city="Paris",  country="FR", lat=48.8566, lng=2.3522, geohash="aaaa"),
            Row(id=2, city="Almaty", country="KZ", lat=43.2389, lng=76.8897, geohash="bbbb"),
        ]
    )

    # Only one geohash has weather
    weather_geo_df = spark.createDataFrame(
        [
            Row(
                geohash="aaaa",
                lat=48.8566,
                lng=2.3522,
                avg_tmpr_c=10.0,
                avg_tmpr_f=50.0,
                wthr_date="2020-01-01",
                year=2020,
                month=1,
                day=1,
            )
        ]
    )

    enriched_df = main_mod.join_restaurants_with_weather(
        restaurants_geo_df,
        weather_geo_df,
    )

    rows = sorted(enriched_df.collect(), key=lambda r: r.id)
    assert len(rows) == 2

    # First restaurant has weather
    r1 = rows[0]
    assert r1.id == 1
    assert r1.avg_tmpr_c == 10.0

    # Second restaurant has no matching weather, columns are NULL
    r2 = rows[1]
    assert r2.id == 2
    assert r2.avg_tmpr_c is None
    assert r2.avg_tmpr_f is None


# ---------- TASK 4: write_enriched_data ----------

def test_write_enriched_data_creates_parquet(spark, tmp_path, monkeypatch):
    """write_enriched_data should write partitioned parquet under BASE_PATH/output/enriched_parquet."""
    # Minimal dataframe with required partition columns
    df = spark.createDataFrame(
        [
            Row(
                geohash="u09t",
                id=1,
                city="Paris",
                country="FR",
                lat=48.8566,
                lng=2.3522,
                avg_tmpr_c=25.0,
                avg_tmpr_f=77.0,
                wthr_date="2020-01-01",
                year=2020,
                month=1,
                day=1,
            )
        ]
    )

    # Redirect BASE_PATH to a temporary directory for the test
    new_base = str(tmp_path)
    monkeypatch.setattr(main_mod, "BASE_PATH", new_base)

    main_mod.write_enriched_data(df)

    output_dir = tmp_path / "output" / "enriched_parquet"
    assert output_dir.exists()

    # There should be at least one parquet file written in some partition
    has_parquet = any(p.suffix == ".parquet" for p in output_dir.rglob("*"))
    assert has_parquet