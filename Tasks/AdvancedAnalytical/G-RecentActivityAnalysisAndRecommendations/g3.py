from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagement')
contentDF.createOrReplaceTempView('content')

# recently watched content withing last week or month
query = """
    SELECT c.* 
    FROM engagement e
    JOIN content c ON e.showid = c.showid
    ORDER BY TO_DATE(e.playbackStarted) DESC
    
"""
sqlDF = spark.sql(query)
sqlDF.show()



# save the output as parquet
output_path = "./output/advanced/G-RecentActivityAnalysisAndRecommendations/3g.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)