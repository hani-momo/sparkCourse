from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from typing import Optional
import codecs
import logging
from enum import Enum
import argparse
import os

'''
HOW TO USE:

spark-submit src/movies_pipeline_practice.py
spark-submit src/movies_pipeline_practice.py --debug --log-level DEBUG
'''

DEBUG_MODE = False
log_level = logging.INFO
# add config path/to/yaml to pass in cli

logger = logging.getLogger(__name__)


RATINGS_SCHEMA = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])


def process_movie_files(
    spark: SparkSession, 
    ratings_path: str,
    movies_path: str,
    encoding: str = 'ISO-8859-1', # default encoding
) -> DataFrame:
    '''Process movie files and return top movies DataFrame'''

    def load_movie_names(path: str) -> dict[int, str]:
        '''Load movie names from file into dict for broadcasting & quick lookup'''
        movie_names = {}
        with codecs.open(path, "r", encoding=encoding) as f:
            for line in f:
                fields = line.split('|')
                movie_names[int(fields[0])] = fields[1]
        return movie_names

    ratings_df = spark.read.option("sep", "\t").schema(RATINGS_SCHEMA).csv(ratings_path)
    
    global debug_mode
    if debug_mode:
        logger.debug("Loading movie names from %s", movies_path)

    movie_names = load_movie_names(movies_path)
    return top_movies(spark, ratings_df, movie_names)

def top_movies(
    spark: SparkSession, 
    ratings_df: DataFrame, 
    movie_names: dict[int, str],
) -> DataFrame:
    '''Process ratings DataFrame to find top movies'''

    movies_counter = ratings_df.groupBy("movieID").count().orderBy(func.desc("count"))

    name_dict = spark.sparkContext.broadcast(movie_names)

    lookup_name_udf = func.udf(lambda movie_id: name_dict.value.get(movie_id))
    result = movies_counter.withColumn("movieTitle", lookup_name_udf(func.col("movieID")))
    
    global debug_mode
    if debug_mode:
        result.explain(True)
        result.show(20, truncate=False)

    return result

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--log-level", type=str, default=logging.INFO, help="Set logging level")
    #parser.add_argument("--config", type=str, help="Pass config path/to/yaml", metavar='FILE') # TODO add config path/to/yaml to pass in cli & load yaml
    args = parser.parse_args()

    global debug_mode,log_level
    debug_mode = args.debug
    log_level = getattr(logging, args.log_level)

def main() -> None:
    parse_args()

    logger.info("Starting Spark application")

    spark = SparkSession.builder.appName("MoviesPipelinePractice").getOrCreate()

    try:
        top_movies = process_movie_files(
            spark=spark,
            ratings_path=os.path.abspath("./files/ml-100k/u.data"),
            movies_path=os.path.abspath("./files/ml-100k/u.item"),
        )
        if debug_mode:
            top_movies.show(10, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

