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
    SELECT c.Genre, COUNT(c.Genre) As MovieCount
    FROM content c
    JOIN users u ON c.ShowID = u.ShowID
    WHERE u.Location = 'New York'
    GROUP BY c.Genre
    ORDER BY MovieCount DESC
    LIMIT 1
"""
joinedDF = spark.sql(query)
joinedDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/popularMovieType.parquet"
joinedDF.write.mode('overwrite').parquet(output_path)