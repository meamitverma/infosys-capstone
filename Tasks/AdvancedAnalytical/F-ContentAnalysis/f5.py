from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# joined table
query = """
    WITH userWatchedGenres AS (
        SELECT u.userid, c.genre, CONCAT_WS(',', COLLECT_LIST(c.showid)) AS shows
        FROM content c
        JOIN users u ON u.showid = c.showid
        GROUP BY c.userid, c.genre
        ORDER BY LENGTH(shows)
    )
"""

sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/content-analysis/showsWitGenreNotWatched.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
