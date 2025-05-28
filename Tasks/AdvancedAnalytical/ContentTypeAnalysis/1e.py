from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# cache users
usersDF.cache()

# create temp view
contentDF.createOrReplaceTempView('content')
usersDF.createOrReplaceTempView('users')

# shows that have not been watched
query = """
    SELECT c.*
    FROM content c
    WHERE c.showid NOT IN (SELECT showid FROM users)
"""

sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/showNeverWatched.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)