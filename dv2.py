from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder \
    .appName("Product Data Analysis") \
    .getOrCreate()

data = [
    (101, "Laptop", "Electronics", 55000, 10, 4.5),
    (102, "Smartphone", "Electronics", 30000, 25, 4.3),
    (103, "Headphones", "Electronics", 2000, 50, 4.1),
    (104, "Chair", "Furniture", 3500, 15, 4.0),
    (105, "Table", "Furniture", 7000, 8, 4.2),
    (106, "Notebook", "Stationery", 60, 200, 4.6),
    (107, "Pen", "Stationery", 20, 500, 4.4),
    (108, "Shoes", "Fashion", 2500, 30, 4.1),
    (109, "T-shirt", "Fashion", 1200, 40, 4.0),
    (110, "Backpack", "Fashion", 1800, 20, 4.3)
]

columns = ["ProductID", "ProductName", "Category", "Price", "StockQuantity", "Rating"]

# creating a DataFrame
df = spark.createDataFrame(data, columns)
print("Original DataFrame:")
df.show()

# Sort by Price (Descending) and Category (Ascending)
sorted_df = df.orderBy(col("Price").desc(), col("Category").asc())
print("Sorted DataFrame (Price DESC, Category ASC):")
sorted_df.show()

# Find Total Sales Amount for Each Category
df_with_sales = df.withColumn(
    "SalesAmount",
    col("Price") * col("StockQuantity")
)

category_sales = df_with_sales.groupBy("Category") \
    .agg(
        _sum("SalesAmount").alias("TotalSalesAmount")
    )
print("Total Sales Amount by Category:")
category_sales.show()

# Find Total Sales Amount and Total Quantity Sold for Each Product
product_sales = df_with_sales.groupBy("ProductID", "ProductName") \
    .agg(
        _sum("SalesAmount").alias("TotalSalesAmount"),
        _sum("StockQuantity").alias("TotalQuantitySold")
    )
print("Total Sales Amount and Quantity Sold for Each Product:")
product_sales.show()

spark.stop()
