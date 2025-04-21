from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import DataFrame
from typing import Optional, List

def process_text(df: DataFrame, text_column: str = "value") -> DataFrame:
    words = df.select(func.explode(func.split(df.value, "\\W+")).alias("word"))
    wordsWithoutEmptyStrings = words.filter(words.word != "")
    lowercaseWords = wordsWithoutEmptyStrings.select(func.lower(wordsWithoutEmptyStrings.word).alias("word"))
    wordCounts = lowercaseWords.groupBy("word").count() # count words' occurrences
    return wordCounts.sort("count")

def run(input_file: str = None, text: Optional[str] = None) -> None:
    spark = SparkSession.builder.appName('WordCountApp').getOrCreate()
    
    if text is not None:
        df = spark.createDataFrame([text], ["value"]) # for tests
    else:
        df = spark.read.text(input_file) # for main read file
    
    result = process_text(df)
    result.show(57, truncate=False)
    spark.stop()

if __name__ == "__main__":
    run(input_file="./files/book.txt")



# from pyspark.sql import SparkSession
# from pyspark.sql import functions as func
# from pyspark.sql import DataFrame

# def process_text(df: DataFrame) -> DataFrame:
#     words = df.select(func.explode(func.split(df.value, "\\W+")).alias("word"))
#     wordsWithoutEmptyStrings = words.filter(words.word != "")
#     lowercaseWords = wordsWithoutEmptyStrings.select(func.lower(wordsWithoutEmptyStrings.word).alias("word"))
#     wordCounts = lowercaseWords.groupBy("word").count() # count words' occurrences
#     return wordCounts.sort("count")


# def run(path_file: str) -> None:
#     spark = SparkSession.builder.appName("WordCount").getOrCreate()
#     input_DF = spark.read.text(path_file)
#     result_DF = process_text(input_DF)
#     result_DF.show(1000, truncate=False)#(50, truncate=False)
#     spark.stop()

# if __name__ == "__main__":
#     run("./files/book.txt")


