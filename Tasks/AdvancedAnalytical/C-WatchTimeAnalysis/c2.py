from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')

# movies with highest average watchtime
query = """
    WITH averageWatchtime AS (
        SELECT showid, AVG(DATEDIFF(playbackStopped, playbackStarted)) AS avgWatchTime
        FROM engagements
        GROUP BY showid
    ),
    maxAvgWatchTime AS (
        SELECT MAX(avgWatchTime) AS maxAvgWatchTime
        FROM averageWatchTime
    )
    SELECT showid, avgWatchTime
    FROM averageWatchTime a
    JOIN maxAvgWatchTime m
    ON a.avgWatchTime = m.maxAvgWatchTime
"""
sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/C-WatchTimeAnalysis/movieWithHighestAverageWatchTime.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)