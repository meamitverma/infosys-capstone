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
    SELECT DISTINCT c.*
    FROM content c
    WHERE c.genre IN (
        SELECT DISTINCT c.genre
        FROM users u
        JOIN content c ON u.showid = c.showid
    )
    AND c.showid NOT IN (
        SELECT u.showid
        FROM users u
    )
"""

sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/content-analysis/showsWitGenreNotWatched.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
