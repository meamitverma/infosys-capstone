from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp views for user and content
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# grouped userid and genre
query = """
    SELECT u.UserID, c.Genre, u.ShowID, u.Timestamp, u.Rating
    FROM users u
    JOIN content c ON u.ShowID = c.ShowID
"""
joinedDF = spark.sql(query)
joinedDF.show()


# save the output as parquet
output_path = "./output/advanced/content-genre/joinedUserContent.parquet"
joinedDF.write.mode('overwrite').parquet(output_path)
