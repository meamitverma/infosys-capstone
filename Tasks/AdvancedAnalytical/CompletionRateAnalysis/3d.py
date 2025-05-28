from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')

# watch history 
query = """
    SELECT userID, AVG(DATEDIFF(TO_DATE(e.playbackStopped), TO_DATE(e.playbackStarted))) AS averageWatchTime, AVG(completionPercent) AS averageCompletionPercent
    FROM engagements e
    GROUP BY e.userID
"""

sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/usersWithAvgWatchTimeAvgCompletionPercent.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)