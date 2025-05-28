from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# joined table
query = """
    SELECT u.userid, c.genre, COUNT(*) AS movieCount
    FROM users u
    JOIN content c ON u.showid = c.showid
    GROUP BY u.userid, c.genre
    ORDER BY movieCount ASC
"""

sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/content-analysis/usersFavoriteGenre.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
