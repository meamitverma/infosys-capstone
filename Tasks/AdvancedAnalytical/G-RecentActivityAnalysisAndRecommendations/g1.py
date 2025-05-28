from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
usersDF.createOrReplaceTempView('users')

# content details from recent watchhistory
query = """
    SELECT UserId, COLLECT_LIST(CONCAT_WS( '|', ShowID , Timestamp, Rating)) AS WatchHistory 
    FROM users group by UserID
"""

sqlDF = spark.sql(query)
sqlDF.show()



# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/1g.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)