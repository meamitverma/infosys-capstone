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

# 
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

query3 = """
    SELECT c.showid, c.genre
    FROM recentWatchedContent r
    JOIN content c ON r.showid = c.showid
    WHERE r.rating >= 4
    GROUP BY c.showid, c.genre
"""

sq1DF = spark.sql(query1)
sql2DF = spark.sql(query2)
recommendDF = spark.sql(query3)

sq1DF.show()
sql2DF.show()
recommendDF.show()



# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/showsWithIdAndYear.parquet"
# sqlDF.write.mode('overwrite').parquet(output_path)