from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
usersDF.createOrReplaceTempView('users')

# recent activity
recentContentDF = spark.sql("SELECT * FROM users ORDER BY timestamp DESC")
recentContentDF.show()

# creating tempview for recentData
recentContentDF.createOrReplaceTempView('recentWatchedContent')

# 
query1 = """
    SELECT c.*
    FROM recentWatchedContent r
    JOIN content c ON r.showid = c.showid
"""

sq1DF = 



# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/showsWithIdAndYear.parquet"
# sqlDF.write.mode('overwrite').parquet(output_path)