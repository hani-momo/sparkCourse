from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from typing import Optional
import codecs
import os


def load_movie_names(
                movies_path: Optional[str] = None, 
                movie_names: Optional[dict[int, str]] = None
            ) -> dict[int, str]:
    if movie_names is not None:
        return movie_names
    if movies_path is not None:
        movie_names = {}
        try:
            with codecs.open("files/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
                for line in f:
                    fields = line.split('|')
                    movie_names[int(fields[0])] = fields[1]
            return movie_names
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found at {path}")
        except Exception as e:
            raise ValueError(f"Error load_movie_names: {e}")

def load_ratings_data(
                    spark: SparkSession, 
                    ratings_path: Optional[str] = None, 
                    ratings_df: Optional[DataFrame] = None # if instead of path we pass a dataframe
                ) -> DataFrame: 
    if ratings_df is not None:
        return ratings_df
    if ratings_path is not None:
        schema = StructType([ \
                                StructField("userID", IntegerType(), True), \
                                StructField("movieID", IntegerType(), True), \
                                StructField("rating", IntegerType(), True), \
                                StructField("timestamp", LongType(), True)])
        return spark.read.option("sep", "\t").schema(schema).csv(ratings_path)

def group_sort_movies(movies_df: DataFrame) -> DataFrame:
    return movies_df.groupBy("movieID").count().orderBy(func.desc("count"))

def process_movie_data(
                    spark: SparkSession, 
                    ratings_path: Optional[str] = None, 
                    movies_path: Optional[str] = None,
                    ratings_df: Optional[DataFrame] = None, # if instead of path we pass a dataframe
                    movie_dict: Optional[dict[int, str]] = None
                ) -> DataFrame:
    if ratings_df is None and ratings_path is None:
        raise ValueError("Ratings_path or ratings_df must be provided")
    if movie_dict is None and movies_path is None:
        raise ValueError("Movies_path or movie_dict must be provided")
    
    ratings_df = load_ratings_data(spark, ratings_path=ratings_path, ratings_df=ratings_df)
    movie_names = load_movie_names(movies_path=movies_path, movie_names=movie_dict)

    sorted_movies = group_sort_movies(ratings_df)

    name_dict = spark.sparkContext.broadcast(movie_names)
    lookup_name_udf = func.udf(lambda movie_id: name_dict.value.get(movie_id))    
    
    return sorted_movies.withColumn("movieTitle", lookup_name_udf(func.col("movieID")))


def main():
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    try:
        top_movies = process_movie_data(
            spark, 
            ratings_path=os.path.abspath("./files/ml-100k/u.data"),
            movies_path=os.path.abspath("./files/ml-100k/u.item")
        )
        top_movies.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()