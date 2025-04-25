from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from typing import Optional
import codecs
import logging
from enum import Enum
import argparse
import os


class LogLevel(Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class AppConfig:
    def __init__(
        self,
        debug_mode: bool = False,
        show_exec_plan: bool = False,
        log_level: LogLevel = LogLevel.INFO
    ):
        self.debug_mode = debug_mode
        self.show_exec_plan = show_exec_plan
        self.log_level = log_level if not debug_mode else LogLevel.DEBUG

    def set_log_level(self) -> None:
        '''Configure logging based on settings'''
        logging.basicConfig(
            level=self.log_level.value,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )

def parse_args() -> AppConfig:
    '''Parse command line arguments'''
    parser = argparse.ArgumentParser(description="Spark Movie Ratings Processor")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--show-plan", action="store_true", help="Show execution plan")
    parser.add_argument("--log-level", type=str, default="INFO", help="Set logging level (INFO, DEBUG)")
    args= parser.parse_args()

    return AppConfig(
        debug_mode=args.debug, 
        show_exec_plan=args.show_plan, 
        log_level=LogLevel(args.log_level))

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
    config: AppConfig,
    encoding: str = 'ISO-8859-1', # default encoding
) -> DataFrame:
    '''Process movie files and return top movies DataFrame'''
    logger = logging.getLogger(__name__)

    def load_movie_names(path: str) -> dict[int, str]:
        '''Load movie names from file into dict for broadcasting & quick lookup'''
        logger.debug("Loading movie names from %s", path)
        movie_names = {}
        with codecs.open(path, "r", encoding=encoding) as f:
            for line in f:
                fields = line.split('|')
                movie_names[int(fields[0])] = fields[1]
        logger.debug("Loaded %d movie names", len(movie_names))
        return movie_names

    try:
        logger.info("Processing movie files...")
        logger.debug("Reading ratings from %s", ratings_path)

        ratings_df = spark.read.option("sep", "\t").schema(RATINGS_SCHEMA).csv(ratings_path)

        if config.show_exec_plan:
            logger.info("Execution plan for Ratings DataFrame:")
            ratings_df.explain(True)
        
        logger.debug("Loading movie names from %s", movies_path)
        movie_names = load_movie_names(movies_path)

        return top_movies(spark, ratings_df, movie_names, config=config)
    
    except Exception as e:
        logger.error(f"Error processing movie files: %s", e, exc_info=config.debug_mode)
        raise

def top_movies(
    spark: SparkSession, 
    ratings_df: DataFrame, 
    movie_names: dict[int, str],
    config: AppConfig
) -> DataFrame:
    '''Process ratings DataFrame to find top movies'''
    logger = logging.getLogger(__name__)
    logger.info("Processing top movies...")

    sorted_movies = ratings_df.groupBy("movieID").count().orderBy(func.desc("count"))

    if config.show_exec_plan:
        logger.info("Execution plan for Top Movies DataFrame:")
        sorted_movies.explain(True)

    name_dict = spark.sparkContext.broadcast(movie_names)
    logger.debug("Broadcasting movie names dict with %d entries", len(name_dict.value))

    lookup_name_udf = func.udf(lambda movie_id: name_dict.value.get(movie_id))
    result = sorted_movies.withColumn("movieTitle", lookup_name_udf(func.col("movieID")))

    if config.debug_mode:
        logger.debug("Top movies sample:") # промежуточные результаты
        sorted_movies.show(20, truncate=False)

    return result

def main() -> None:
    config = parse_args()
    config.set_log_level()
    logger = logging.getLogger(__name__)

    logger.info("Starting Spark application with config: %s", config)
    spark = (SparkSession.builder.appName("MoviesPipelinePractice")
    .config("spark.sql.adaptive.enabled", "true"))
    .getOrCreate()

    try:
        logger.info("Spark Application started with config: %s", vars(config))
        top_movies = process_movie_files(
            spark=spark,
            ratings_path=os.path.abspath("./files/ml-100k/u.data"),
            movies_path=os.path.abspath("./files/ml-100k/u.item"),
            config=config
        )
        logger.info("Top 10 Movies Dataframe:")
        top_movies.show(10, truncate=False)
    
    except Exception as e:
        logger.critical("Application failed: %s", e, exc_info=config.debug_mode)
        raise
    finally:
        spark.stop()
        logger.info("Spark Application stopped")

if __name__ == "__main__":
    main()

# spark-submit movies_pipeline_practice.py --debug --show-plan --log-level DEBUG