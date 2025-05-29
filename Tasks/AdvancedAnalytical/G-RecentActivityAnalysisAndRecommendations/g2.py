from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
usersDF.createOrReplaceTempView('users')

# group userid with watchhistoty by timstamp in desc order
query = """
    SELECT UserID, TO_DATE(Timestamp,'yyyy-MM-dd') AS LastDateWatched 
    FROM users 
    ORDER BY UserID, LastDateWatched DESC
"""
sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/G-RecentActivityAnalysisAndRecommendations/watchhistoryDescendingOrder.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)