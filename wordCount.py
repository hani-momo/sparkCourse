from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("./files/book.txt")
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyStrings = words.filter(words.word != "")

lowercaseWords = wordsWithoutEmptyStrings.select(func.lower(wordsWithoutEmptyStrings.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count() # count words' occurrences
wordCountsSorted = wordCounts.sort("count")

wordCountsSorted.show(50, truncate=False)
#wordCountsSorted.show(wordCountsSorted.count())

spark.stop()
