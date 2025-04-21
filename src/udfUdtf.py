from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re

@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        """Extracts hashtags from the input text."""
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)

@udf(returnType=IntegerType())
def count_hashtags(text: str):
    """Counts the number of hashtags in the input text."""
    if text:
        return len(re.findall(r"#\w+", text))
    return 0


spark = SparkSession.builder \
    .appName("Python UDTF and UDF Example") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()

spark.udtf.register("extract_hashtags", HashtagExtractor)

spark.udf.register("count_hashtags", count_hashtags)

#Usage
print('EXTRACT ####, UDTF EXAMPLE')
spark.sql("SELECT * FROM extract_hashtags('Welcome to #ApacheSpark and #BigData!')").show()

print('\nCOUNT ####, UDF EXAMPLE')
spark.sql("SELECT count_hashtags('Welcome to #ApacheSpark and #BigData!') AS hashtag_count").show()

#Usage on df
data = [("Learning #AI with #ML",), ("Explore #DataScience",), ("No hashtags here",)]
df = spark.createDataFrame(data, ["text"])
df.selectExpr("text", "count_hashtags(text) AS num_hashtags").show()


print("\nUDTF usage with LATERAL JOIN:")
df.createOrReplaceTempView("tweets")
spark.sql(
    "SELECT text, hashtag FROM tweets, LATERAL extract_hashtags(text)"
).show()
