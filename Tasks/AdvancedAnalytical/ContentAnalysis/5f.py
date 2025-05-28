from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# joined table
query = """
    SELECT showID, avg(Rating) as averageRating 
    FROM users 
    GROUP BY showid 
    ORDER BY averageRating DESC
"""

sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/content-analysis/showsWithHighestAverageRating.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
