from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("RealEstatePricePrediction") \
    .getOrCreate()

df = spark.read.csv(
    "Datasets/house_data.csv",
    header=True,
    inferSchema=True
)
df.printSchema()

# Handle Missing Values
df = df.dropna()
# Automatically select feature columns (All columns except Price)
feature_columns = [c for c in df.columns if c != "Price"]
# Assemble Features
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)
df_features = assembler.transform(df)
# Scale Features
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures"
)
df_scaled = scaler.fit(df_features).transform(df_features)

# Train-Test Split
train_df, test_df = df_scaled.randomSplit([0.8, 0.2], seed=42)

# Train Linear Regression Model
lr = LinearRegression(
    featuresCol="scaledFeatures",
    labelCol="Price"
)
model = lr.fit(train_df)

# Predictions
predictions = model.transform(test_df)
predictions.select("Price", "prediction").show(5)

# Evaluate Model (RMSE)
evaluator = RegressionEvaluator(
    labelCol="Price",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
print("RMSE:", rmse)

# Feature Importance
print("\nFeature Coefficients:")
for f, c in zip(feature_columns, model.coefficients):
    print(f, ":", c)
    
spark.stop()
