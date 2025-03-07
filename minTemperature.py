from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv("./files/1800.csv") # read file as df
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")
stationTemps = minTemps.select("stationID", "temperature")

minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

minTempsByStationF = minTempsByStation.withColumn("temperature", # convert t to fahrenheit and sort
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2)
                                                  .select("stationID", "temperature").sort("temperature"))

results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()
