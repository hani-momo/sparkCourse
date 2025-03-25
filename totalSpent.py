from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

customerOrderSchema = StructType([ \
                                  StructField("customer_id", IntegerType(), True),
                                  StructField("item_id", IntegerType(), True),
                                  StructField("amount_spent", FloatType(), True)
                                  ])

customersDF = spark.read.schema(customerOrderSchema).csv("files/customer-orders.csv")

totalByCustomer = customersDF.groupBy("customer_id").agg(func.round(func.sum("amount_spent"), 2)\
                                      .alias("total_spent"))
totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
