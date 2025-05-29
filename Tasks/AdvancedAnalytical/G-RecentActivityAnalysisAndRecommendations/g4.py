from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# recent activity
recentContentDF = spark.sql("SELECT * FROM users ORDER BY timestamp DESC")
recentContentDF.show()

# creating tempview for recentData
recentContentDF.createOrReplaceTempView('recentWatchedContent')

# create temp view
engagementDF.createOrReplaceTempView('engagement')
userDF.createOrReplaceTempView('users')

# 
query = """
    WITH recentlyWatched AS (
        SELECT u.UserID, e.ShowID, TO_DATE(playbackStopped,'yyyy-MM-dd') AS LastDateWatched, Rating 
        FROM engagement e
        JOIN users u ON e.showid = u.showid
        ORDER BY LastDateWatched DESC LIMIT 10
    )
    SELECT * 
    FROM recentlyWatched
    ORDER BY Rating DESC 
    LIMIT 10
"""

sqlDF = spark.sql(query)
sqlDF.show()



# save the output as parquet
output_path = "./output/advanced/G-RecentActivityAnalysisAndRecommendations/4g.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)