from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp views for user and content
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')
engagementDF.createOrReplaceTempView('engagements')

# join user, content and engagement
# engagementWatchTime = spark.sql()
query = """
    SELECT u.UserID, c.Genre, AVG(u.Rating) AS AverageRating, SUM(DATEDIFF(e.PlaybackStopped, e.PlaybackStarted)) AS TotalWatchTime
    FROM engagements e
    JOIN users u ON e.UserID = u.UserID
    JOIN content c ON e.ShowID = c.ShowID
    GROUP BY u.UserID, c.Genre
"""
joinedDF = spark.sql(query)
joinedDF.show()


# # save the output as parquet
output_path = "./output/advanced/content-genre/avgRatingTotalWatchTimeGroupedWithUserAndGenre.parquet"
joinedDF.write.mode('overwrite').parquet(output_path)
