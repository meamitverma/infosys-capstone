from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# popular movie type in New York
query = """
    WITH movieCount AS (
        SELECT c.genre, COUNT(c.genre) As movieCount
        FROM content c
        JOIN users u ON c.ShowID = u.ShowID
        WHERE u.Location = 'New York'
        GROUP BY c.Genre
        ORDER BY MovieCount DESC
    ),
    maxCount AS (
        SELECT MAX(movieCount) AS maxMovieCount
        FROM movieCount
    )
    SELECT s.genre, s.movieCount
    FROM movieCount s
    JOIN maxCount h ON s.movieCount = h.maxMovieCount
"""
genreDF = spark.sql(query)
genreDF.show()


# save the output as parquet
output_path = "./output/advanced/B-demographic-and-subscription-tier/popularMovieType.parquet"
genreDF.write.mode('overwrite').parquet(output_path)