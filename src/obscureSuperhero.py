from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("files/Marvel+Names.txt")
lines = spark.read.text("files/Marvel+Graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
minConnectionCount = connections.agg(func.min("connections")).first()[0]
minConnections = connections.filter(func.col("connections") == minConnectionCount)
minConnectionsWithNames = minConnections.join(names, "id")

print('\n\n\n\n' + "The following characters have only " + str(minConnectionCount) + " connection(s):")
minConnectionsWithNames.select("name").show()

spark.stop()