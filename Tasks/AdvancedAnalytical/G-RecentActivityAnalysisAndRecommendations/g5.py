from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
usersDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# recent activity
recentContentDF = spark.sql("SELECT * FROM users ORDER BY timestamp DESC")
recentContentDF.show()

# creating tempview for recentData
recentContentDF.createOrReplaceTempView('recentWatchedContent')

# content details from recent watchhistory
query1 = """
    SELECT r.userid, c.genre, c.actors, c.director
    FROM recentWatchedContent r
    JOIN content c ON r.showid = c.showid
"""
sql1DF = spark.sql(query1)
sql1DF.show()

# number of unique users watching specific genre or actor
query2="""
    SELECT actor, COUNT(DISTINCT userid) AS uniqueUsers
    FROM (
        SELECT r.userid, c.showid, EXPLODE(SPLIT(c.actors, '\\\|')) AS actor
        FROM content c
        JOIN recentWatchedContent r ON r.showid = c.showid
    ) t
    GROUP BY actor
    ORDER BY uniqueUsers DESC
"""
sql2DF = spark.sql(query2)
sql2DF.show()

# recommend content on recenlty watched with high ratings
query3 = """
    SELECT c.showid, c.genre
    FROM recentWatchedContent r
    JOIN content c ON r.showid = c.showid
    WHERE r.rating >= 4
    GROUP BY c.showid, c.genre
"""
recommendDF = spark.sql(query3)
recommendDF.show()


# save the output as parquet
output_path = "./output/advanced/G-RecentActivityAnalysisAndRecommendations/"
sql1DF.write.mode('overwrite').parquet(output_path + 'contentDetailsRecentWatchHistory.parquet')
sql2DF.write.mode('overwrite').parquet(output_path + 'uniqueUsersWatchingSpecificGenre.parquet')
recommendDF.write.mode('overwrite').parquet(output_path + 'contentRecommendations.parquet')