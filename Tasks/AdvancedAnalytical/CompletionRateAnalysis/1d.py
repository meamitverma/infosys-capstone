from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')

# user with most completed movies
query = """
    SELECT userid, completionpercent
    FROM engagements
    WHERE completionpercent = 100
"""
sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/showsWithIdAndYear.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)