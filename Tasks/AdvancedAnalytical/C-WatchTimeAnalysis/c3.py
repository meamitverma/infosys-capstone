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
sql1DF = spark.sql(query1)
sql1DF.show()


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
    SELECT w.showid, w.watchTime
    FROM watchTime w
    JOIN maxWatchTime h ON w.watchTime = h.maxWatchTime
    UNION
    SELECT w.showid, w.watchTime
    FROM watchTime w
    JOIN minWatchTime l ON w.watchTime = l.minWatchTime
"""
sql2DF = spark.sql(query2)
sql2DF.show()


# save the output as parquet
output_path = "./output/advanced/C-WatchTimeAnalysis/"
sql1DF.write.mode('overwrite').parquet(output_path+'usersWithLongestWatchTime.parquet')
sql2DF.write.mode('overwrite').parquet(output_path+'showsWithHigherLowerAvgWatchTime.parquet')