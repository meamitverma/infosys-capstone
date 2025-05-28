from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')
usersDF.createOrReplaceTempView('users')

# watch history 
query = """
    SELECT u.userID, AVG(DATEDIFF(e.playbackStopped, e.playbackStarted)) AS averageWatchTime, AVG(completionPercent) AS averageCompletionPercent
    FROM users u
    JOIN engagements e ON u.userID = e.userID
    GROUP BY u.userID
"""

sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/usersWithAvgWatchTimeAvgCompletionPercent.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)