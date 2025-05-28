from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
contentDF.createOrReplaceTempView('content')
userDF.createOReplaceTempView('users')

# shows that have not been watched
query = """
    SELECT * 
"""

joinedDF = spark.sql(query)
joinedDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/showNeverWatched.parquet"
joinedDF.write.mode('overwrite').parquet(output_path)