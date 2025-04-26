from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs
import logging
import argparse
import yaml
import os

'''
HOW TO USE:

spark-submit src/movies_pipeline_practice.py --debug --log-level DEBUG
spark-submit src/movies_pipeline_practice.py --log-level INFO
or see Makefile
'''

DEBUG = False
LOG_LEVEL = logging.INFO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    encoding: str = 'ISO-8859-1',
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
    
    if DEBUG:
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
    
    if DEBUG:
        result.explain(True)
        result.show(20, truncate=False)

    return result

def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--log-level", type=str, default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set logging level") # logging.INFO returns an int 20
    parser.add_argument("--config", type=str, default=None, help='YAML config path')
    args = parser.parse_args()

    global DEBUG,LOG_LEVEL
    DEBUG = args.debug
    LOG_LEVEL = getattr(logging, args.log_level.upper())
    return args

def main() -> None:
    args = parse_args()
    
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    config = {
        "debug": DEBUG,
        "logging": {
            "level": logging.getLevelName(LOG_LEVEL)
        },
    }
    if args.config:
        yaml_config = load_config(args.config)
        for key, value in yaml_config.items():
            if isinstance(value, dict) and key in config:
                config[key].update(value)
            else:
                config[key] = value

    logger.info(f"Starting Spark application with config: {config}")
    
    spark_builder = SparkSession.builder.appName(config["spark_config"]["app_name"])
    spark = (SparkSession.builder
             .appName(config["spark_config"]["app_name"])
             .config("spark.sql.shuffle.partitions", config["spark_config"]["config"]["spark.sql.shuffle.partitions"])
             .config("spark.sql.adaptive.enabled", config["spark_config"]["config"]["spark.sql.adaptive.enabled"])
             .getOrCreate())

    try:
        top_movies = process_movie_files(
            spark=spark,
            ratings_path=config["data_paths"]["ratings"],
            movies_path=config["data_paths"]["movies"],
            encoding="ISO-8859-1"
        )

        if DEBUG:
            top_movies.show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

