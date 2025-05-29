from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# shows with id Mxx and year 20xx
query = """
    WITH filteredShows AS (
        SELECT u.showid, COUNT(u.showid) AS watchcount
        FROM users u
        WHERE u.showid LIKE 'M%' AND u.timestamp LIKE '20%'
        GROUP BY u.showid
    )
    SELECT c.showid, c.genre, c.director, f.watchCount
    FROM content c
    JOIN filteredShows f ON f.showid = c.showid
    ORDER BY c.showid ASC
"""
showsDF = spark.sql(query)
showsDF.show()


# save the output as parquet
output_path = "./output/advanced/C-WatchTimeAnalysis/showsWithIdAndYear.parquet"
showsDF.write.mode('overwrite').parquet(output_path)