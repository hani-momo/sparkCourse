from pyspark.sql import SparkSession, DataFrame, functions as func
from typing import Optional


def load_friends_data(
    spark: SparkSession,
    file_path: Optional[str] = None,
    friends_df: Optional[DataFrame] = None
) -> DataFrame:
    if friends_df is not None:
        return friends_df
    elif file_path is not None:
        try:
            return spark.read.option("header", "true") \
                             .option("inferSchema", "true") \
                             .csv(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found at {file_path}")
        except Exception as e:
            raise ValueError(f"Error loading friends data: {e}")
    else:
        raise ValueError("Either file_path or friends_df must be provided")
    

def process_friends_data(
    spark: SparkSession, 
    file_path: Optional[str] = None,
    friends_df: Optional[DataFrame] = None # modify code
) -> DataFrame:
    try:
        df = load_friends_data(spark=spark, file_path=file_path, friends_df=friends_df)
        if "age" not in lines.columns or "friends" not in lines.columns: # ??
            raise ValueError("Missing age or friends columns")
        
        result = df.select("age", "friends")\
            .groupBy("age").agg(func.avg("friends").alias("friends_avg")) \
            .sort("age")
        
        return result

    except Exception as e:
        raise ValueError(f"Error processing friends data: {e}") from e


def main() -> None:
    spark = SparkSession.builder.appName("friendsByAge").getOrCreate()
    try:
        result = process_friends_data(
            spark=spark, 
            file_path="files/fakefriends-header.csv",
        )
        result.show(10, truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()