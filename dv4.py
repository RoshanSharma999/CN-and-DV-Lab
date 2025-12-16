from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder \
    .appName("Airline Delay Analysis") \
    .getOrCreate()

df = spark.read.csv(
    "Datasets/airline_data.csv",
    header=True,
    inferSchema=True
)
df.printSchema()

# grouped data by airline and its average delay
avg_delay_by_carrier = df.groupBy("carrier") \
    .agg(
        avg("arr_delay").alias("AverageArrivalDelay")
    ) \
    .orderBy(col("AverageArrivalDelay").desc())
print("Average Arrival Delay by Airline Carrier:")
avg_delay_by_carrier.show()

# top 5 routes with highest average delay
avg_delay_by_route = df.groupBy("origin", "dest") \
    .agg(
        avg("arr_delay").alias("AverageArrivalDelay")
    ) \
    .orderBy(col("AverageArrivalDelay").desc()) \
    .limit(5)
print("Top 5 Routes with Highest Average Delay:")
avg_delay_by_route.show()

spark.stop()
