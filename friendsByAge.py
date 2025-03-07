from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

try:
    lines = spark.read.option("header", "true").option("inferSchema", "true").csv("./files/fakefriends-header.csv")
    
    if "age" not in lines.columns or "friends" not in lines.columns:
        print("No such columns")
    else:
        friendsByAge = lines.select("age", "friends")
        friendsByAge.groupBy("age").avg("friends").sort("age").show() # sorted
        friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2) # rounded & separate column name
                                        .alias("friends_avg")).sort("age").show()

except Exception as e:
    print(f"ERR OCCURED: {e}")

finally:
    spark.stop()
