from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# genre popular in age group 18 to 30
query = """
    SELECT c.Genre, count(Genre) AS Count
    FROM users u
    JOIN content c ON u.ShowID = c.ShowID
    WHERE age BETWEEN 18 AND 30
    GROUP BY c.Genre
    ORDER BY Count DESC
    LIMIT 1
"""
genreDF = spark.sql(query)
genreDF.show()


# save the output as parquet
output_path = "./output/advanced/B-demographic-and-subscription-tier/popularGenreInAgeGroup.parquet"
genreDF.write.mode('overwrite').parquet(output_path)