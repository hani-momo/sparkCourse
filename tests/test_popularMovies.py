import pytest
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from src.popularMovies import (
    load_movie_names,
    load_ratings_data,
    group_sort_movies,
    process_movie_data,
)


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TestPopularMovies")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def sample_movie_dict() -> dict[int, str]:
    return {
        1: "Toy Story (1995)",
        2: "GoldenEye (1995)",
        3: "Four Rooms (1995)"
    }

@pytest.fixture
def valid_ratings_df(spark_session: SparkSession) -> DataFrame:
    data = [
        Row(userID=1, movieID=1, rating=5, timestamp=100),
        Row(userID=1, movieID=1, rating=4, timestamp=200),
        Row(userID=2, movieID=2, rating=3, timestamp=300),
        Row(userID=3, movieID=1, rating=5, timestamp=400)
    ]
    schema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def invalid_ratings_df(spark_session: SparkSession) -> DataFrame:
    data = [
        Row(user_id=1, movie_id=101, score=5, date=100),
        Row(user_id=2, movie_id=101, score=4, date=200)
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("dummy_id", IntegerType(), True),
        StructField("score", IntegerType(), True),
        StructField("date", LongType(), True)
    ])
    return spark_session.createDataFrame(data, schema)



# --------- Test Cases ---------

def test_load_ratings_data_both_inputs(spark_session, valid_ratings_df):
    result = load_ratings_data(spark_session, ratings_path="dummy_path", ratings_df=valid_ratings_df)
    assert result.count() == valid_ratings_df.count()

def test_process_movie_data_neither_input(spark_session: SparkSession):
    with pytest.raises(ValueError):
        process_movie_data(spark_session)

def test_process_movie_data_with_both_inputs(spark_session, valid_ratings_df, sample_movie_dict):
    result = process_movie_data(
        spark_session,
        ratings_path="dummy_path",
        movies_path="dummy_path",
        ratings_df=valid_ratings_df,
        movie_dict=sample_movie_dict
    )
    assert result.count() == 2

def test_group_sort_movies_invalid_schema(spark_session, invalid_ratings_df):
    with pytest.raises(Exception):
        group_sort_movies(invalid_ratings_df)


