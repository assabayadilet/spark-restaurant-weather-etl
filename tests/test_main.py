import pytest
from pyspark.sql import SparkSession, Row

import jobs.main as main_mod  # импортируем модуль, а не отдельные функции


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
    # детерминированность
    assert main_mod.geohash_4(lat, lng) == gh


def test_geohash_4_none_safe():
    gh = main_mod.geohash_4(None, 10.0)
    assert gh is None
    gh = main_mod.geohash_4(10.0, None)
    assert gh is None


# ---------- TASK 1: process_restaurant_coordinates ----------

def test_process_restaurant_coordinates_split_and_geocode(spark, monkeypatch):
    # Подготовим маленький DataFrame:
    #  id=1 — валидная запись
    #  id=2,3 — с некорректными координатами
    data = [
        Row(id=1, city="Paris",  country="FR", lat=48.8566, lng=2.3522),
        Row(id=2, city="Dillon", country="US", lat=None,     lng=-79.38),
        Row(id=3, city="Dillon", country="US", lat=34.40,    lng=None),
    ]
    df = spark.createDataFrame(data)

    # Мокаем геокодинг, чтобы НЕ ходить в реальный OpenCage
    def fake_geocode(city, country):
        return 1.23, 4.56

    monkeypatch.setattr(main_mod, "geocode_with_opencage", fake_geocode)

    valid_df = main_mod.process_restaurant_coordinates(df)

    # Проверяем, что в валидных осталась только запись с id=1
    valid_ids = {row.id for row in valid_df.collect()}
    assert valid_ids == {1}


# ---------- TASK 2: add_geohash_columns ----------

def test_add_geohash_columns_adds_geohash(spark):
    # Один ресторан с координатами
    restaurants = spark.createDataFrame(
        [Row(id=1, city="Paris", country="FR", lat=48.8566, lng=2.3522)]
    )

    # Одна запись погоды с такими же координатами
    weather = spark.createDataFrame(
        [Row(lat=48.8566, lng=2.3522, year=2020, month=1, day=1)]
    )

    restaurants_geo_df, weather_geo_df = main_mod.add_geohash_columns(
        restaurants,
        weather,
    )

    # Проверяем, что колонка geohash добавлена
    assert "geohash" in restaurants_geo_df.columns
    assert "geohash" in weather_geo_df.columns

    r_row = restaurants_geo_df.select("geohash").first()
    w_row = weather_geo_df.select("geohash").first()

    # geohash не пустой и в обоих датафреймах совпадает
    assert r_row.geohash is not None
    assert w_row.geohash is not None
    assert len(r_row.geohash) == 4
    assert r_row.geohash == w_row.geohash