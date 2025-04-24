import pytest
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from src.popularMovies import (
    process_movie_files,
    top_movies,
    RATINGS_SCHEMA,
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
def input_sample_movie_dict() -> dict[int, str]:
    return {
        1: "Toy Story (1995)",
        2: "GoldenEye (1995)",
        3: "Four Rooms (1995)"
    }

@pytest.fixture
def input_sample_ratings_df(spark_session: SparkSession) -> DataFrame:
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
    return spark_session.createDataFrame(data, RATINGS_SCHEMA)


# --------- Test Cases ---------

def test_find_most_popular_movies(spark_session, input_sample_ratings_df, input_sample_movie_dict):
    ''' Test top_movies func '''
    result = top_movies(spark_session, input_sample_ratings_df, input_sample_movie_dict)
    assert result.first()["movieID"] == 1
