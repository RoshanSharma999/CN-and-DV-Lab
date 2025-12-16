from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MovieRatingsSQL") \
    .getOrCreate()

movies_path = "Datasets/movies.csv"
ratings_path = "Datasets/ratings.csv"

movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)

print("Movies Data:")
movies_df.show(5)

print("Ratings Data:")
ratings_df.show(5)

# temporary SQL views
movies_df.createOrReplaceTempView("movies")
ratings_df.createOrReplaceTempView("ratings")

# Top 10 Highest Rated Movies with at Least 10 Ratings
top_movies_query = """
SELECT
    m.movieId,
    m.title,
    COUNT(r.rating) AS num_ratings,
    AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title
HAVING COUNT(r.rating) >= 10
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 10
"""

top_movies = spark.sql(top_movies_query)

print("Top 10 Highest Rated Movies (with ≥ 10 ratings):")
top_movies.show(truncate=False)

# Find Most Active Users (Users Who Rated the Most Movies)
active_users_query = """
SELECT
    userId,
    COUNT(rating) AS total_ratings
FROM ratings
GROUP BY userId
ORDER BY total_ratings DESC
"""

active_users = spark.sql(active_users_query)

print("Most Active Users:")
active_users.show(10)

spark.stop()
