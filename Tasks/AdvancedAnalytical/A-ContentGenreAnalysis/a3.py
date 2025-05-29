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

# average rating and total watch time grouped by userid and genre
query = """
    SELECT u.UserID, c.Genre, AVG(u.Rating) AS AverageRating, SUM(DATEDIFF(TO_DATE(e.PlaybackStopped,'yyyy-MM-dd'), TO_DATE(e.PlaybackStarted,'yyyy-MM-dd'))) AS TotalWatchTime
    FROM engagements e
    JOIN users u ON e.UserID = u.UserID
    JOIN content c ON e.ShowID = c.ShowID
    GROUP BY u.UserID, c.Genre
"""
sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/content-genre/avgRatingTotalWatchTimeGroupedWithUserAndGenre.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
