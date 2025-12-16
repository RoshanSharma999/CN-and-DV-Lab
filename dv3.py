from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Airline Flight Analysis") \
    .getOrCreate()

# Load CSV file
df = spark.read.csv(
    "Datasets/airline_data.csv",
    header=True,
    inferSchema=True
)

# Display schema to verify
df.printSchema()
# Show sample data
df.show(5)

# flights delayed more than 15 minutes
delayed_flights = df.filter(col("arr_delay") > 15)
print("Flights delayed by more than 15 minutes:")
delayed_flights.select(
    "flight", "carrier", "origin", "dest", "distance", "air_time", "arr_delay"
).show(10)

# correlation between the flight length and the likelihood of a delay
# using distance
distance_delay_corr = df.stat.corr("distance", "arr_delay")
print("Correlation between distance and arrival delay:", distance_delay_corr)
# using air_time
airtime_delay_corr = df.stat.corr("air_time", "arr_delay")
print("Correlation between air time and arrival delay:", airtime_delay_corr)

spark.stop()
