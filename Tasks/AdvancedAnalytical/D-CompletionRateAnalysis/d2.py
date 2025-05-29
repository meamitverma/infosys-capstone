from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')

# shows with average completion rate
query = """
    SELECT showid, AVG(completionPercent) AS AverageCompletionPercent
    FROM engagements
    GROUP BY showid
"""
sqlDF = spark.sql(query)
sqlDF.show()

# repartion based on showid
repartitionedDF = sqlDF.repartition('showid')

# save the output as parquet
output_path = "./output/advanced/D-CompletionRateAnalysis/showsWithAverageCompletionPercent.parquet"
repartitionedDF.write.mode('overwrite').parquet(output_path)