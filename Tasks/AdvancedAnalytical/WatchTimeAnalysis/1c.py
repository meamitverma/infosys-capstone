from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# shows with id Mxx and year 20xx
query = """
    SELECT u.showid, u.timestamp, COUNT(u.showid) AS watchcount
    FROM users u
    WHERE u.showid LIKE 'M%' AND u.timestamp LIKE '20%'
    GROUP BY u.showid, u.timestamp
"""
sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/showsWithIdAndYear.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)