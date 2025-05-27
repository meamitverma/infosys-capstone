from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col,count

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# comedymovie names watched by users
query = """
    SELECT DISTINCT(u.showid), c.genre
    FROM users u
    JOIN content c ON u.showid = c.showid
    WHERE c.genre = 'Comedy'
"""
sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/demographicAndSubscriptionTier/comedyMovieWatched.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)