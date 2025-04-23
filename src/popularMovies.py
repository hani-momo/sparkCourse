from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from typing import Optional
import codecs
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RATINGS_SCHEMA = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])


def process_movie_files(spark: SparkSession, movies_path: str, encoding='ISO-8859-1') -> dict[int, str]:
    def load_movie_names(path: str) -> dict[int, str]:
        movie_names = {}
        with codecs.open(path, "r", encoding=encoding) as f:
            for line in f:
                fields = line.split('|')
                movie_names[int(fields[0])] = fields[1]
        return movie_names

    try:
        ratings_df = spark.read.option("sep", "\t").schema(RATINGS_SCHEMA).csv(ratings_path)
        movie_names = load_movie_names(movies_path)
        
        return top_movies(ratings_df, movie_names)
    
    except Exception as e:
        logger.error(f"Error processing movie files: {e}")
        raise e

def top_movies(spark: SparkSession, ratings_df: DataFrame, movie_names: dict[int, str]) -> DataFrame:
    sorted_movies = ratings_df.groupBy("movieID").count().orderBy(func.desc("count"))

    name_dict = spark.sparkContext.broadcast(movie_names) 
    lookup_name_udf = func.udf(lambda movie_id: name_dict.value.get(movie_id))

    return sorted_movies.withColumn("movieTitle", lookup_name_udf(func.col("movieID")))

def main() -> None:
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    try:
        top_movies = process_movie_files(
            spark=spark,
            ratings_path=os.path.abspath("./files/ml-100k/u.data"),
            movies_path=os.path.abspath("./files/ml-100k/u.item")
        )
        top_movies.show(10, truncate=False)
    except Exception as e:
        logger.error(f"Error {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
