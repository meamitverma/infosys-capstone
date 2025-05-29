from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# average rating by age group
query = """
    SELECT u.Age, AVG(u.Rating) AS AvgRating
    FROM users u
    WHERE age > 45
    GROUP BY u.Age
"""
ratingDF = spark.sql(query)
ratingDF.show()


# save the output as parquet
output_path = "./output/advanced/B-demographic-and-subscription-tier/averageRatingByAge.parquet"
ratingDF.write.mode('overwrite').parquet(output_path)