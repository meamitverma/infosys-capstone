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
    WITH 
        SELECT c.Genre, COUNT(c.Genre) As MovieCount
        FROM content c
        JOIN users u ON c.ShowID = u.ShowID
        WHERE u.Location = 'New York'
        GROUP BY c.Genre
        ORDER BY MovieCount DESC
"""
genreDF = spark.sql(query)
genreDF.show()


# save the output as parquet
output_path = "./output/advanced/B-demographic-and-subscription-tier/popularMovieType.parquet"
genreDF.write.mode('overwrite').parquet(output_path)