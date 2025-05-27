from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')

# byuserid: users with longer watchtime
query1 = """
    WITH watchTime AS (
        SELECT userid, SUM(DATEDIFF(playbackStopped, playbackStarted)) AS watchTime
        FROM engagements
        GROUP BY userid
    ),
    maxWatchTime AS (
        SELECT MAX(watchTime) AS maxWatchTime
        FROM watchTime
    )
    SELECT userid, watchTime
    FROM watchTime w
    JOIN maxWatchTime m
    ON w.watchTime = m.maxWatchTime
"""
# byshowid: shows with higher or lower average watchtime
query2 = """
    WITH watchTime AS (
        SELECT showid, SUM(DATEDIFF(playbackStopped, playbackStarted)) AS watchTime
        FROM engagements
        GROUP BY showid
    ),
    maxWatchTime AS (
        SELECT MAX(watchTime) AS maxWatchTime
        FROM watchTime
    ),
    minWatchTime AS (
        SELECT MIN(watchTime) AS minWatchTime
        FROM watchTime
    )
    SELECT showid, watchTime
    FROM watchTime w, maxWatchTime h, minWatchTime l
    WHERE w.watchTime = h.maxWatchTime OR w.watchTime = l.minWatchTime
"""

sql1DF = spark.sql(query1)
sql2DF = spark.sql(query2)

sql1DF.show()
sql2DF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/"
sql1DF.write.mode('overwrite').parquet(output_path+'usersWithLongestWatchTime.parquet')
sql2DF.write.mode('overwrite').parquet(output_path+'showsWithHigherLowerAvgWatchTime.parquet')