from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagement')

# 
query = """
    SELECT UserID, ShowID, TO_DATE(playbackStopped,'yyyy-MM-dd') AS LastDateWatched 
    FROM engagement 
    ORDER BY LastDateWatched DESC 
    LIMIT 10
"""

sqlDF = spark.sql(query)
sqlDF.show()



# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/3g.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)