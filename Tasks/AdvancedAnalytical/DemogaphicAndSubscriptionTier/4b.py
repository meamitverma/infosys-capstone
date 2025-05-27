from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# shows with premium subscription
query = """
    SELECT u.ShowID, u.Subscription
    FROM users u
    WHERE u.Subscription = 'Premium'
"""
joinedDF = spark.sql(query)
joinedDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/premiumUsersShowID.parquet"
joinedDF.write.mode('overwrite').parquet(output_path)