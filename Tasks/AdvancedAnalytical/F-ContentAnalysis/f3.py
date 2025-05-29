from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# shows with high ratings
query = """
    SELECT u.userid, u.rating, c.*
    FROM users u
    JOIN content c ON u.showid = c.showid
    WHERE rating = 5
"""
sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/F-ContentAnalysis/showsWithHighRating.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
