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
subscriptionDF = spark.sql(query)
subscriptionDF.show()


# save the output as parquet
output_path = "./output/advanced/B-demographic-and-subscription-tier/premiumUsersShowID.parquet"
subscriptionDF.write.mode('overwrite').parquet(output_path)