from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("StudentPerformance") \
    .getOrCreate()

data = [
    ("Alice", 25, "Bangalore", "Female"),
    ("Bob", 32, "Mumbai", "Male"),
    ("Charlie", 45, "Delhi", "Male"),
    ("Diana", 29, "Chennai", "Female"),
    ("Ethan", 38, "Hyderabad", "Male"),
    ("Fiona", 41, "Pune", "Female"),
    ("George", 22, "Mumbai", "Male"),
    ("Hannah", 34, "Kolkata", "Female"),
    ("Ian", 28, "Delhi", "Male"),
    ("Jenny", 36, "Bangalore", "Female")
]

columns = ["name", "age", "city", "gender"]

# creating a DataFrame
df = spark.createDataFrame(data, columns)
print("Original DataFrame:")
df.show()

# Filter rows with age greater than 30
filtered_df = df.filter(col("age") > 30)
print("Filtered DataFrame (age > 30):")
filtered_df.show()

# Add a new column named “tax”
# Example: tax = age * 100 (any logic you want)
df_with_tax = df.withColumn("tax", col("age") * 100)
print("DataFrame with new tax column:")
df_with_tax.show()

# Rename “age” column to “years”
df_renamed = df_with_tax.withColumnRenamed("age", "years")
print("Renamed DataFrame (age → years):")
df_renamed.show()

# Drop Multiple Columns
df_dropped = df_renamed.drop("city", "gender")
print("DataFrame after dropping city & gender:")
df_dropped.show()

spark.stop()
