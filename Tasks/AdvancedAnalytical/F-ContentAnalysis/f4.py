from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# joined table
query = """
    WITH userGenreRank AS (
        SELECT UserID, Genre, CAST(Rating aS INT) AS Rating, RANK() OVER (PARTITION BY UserID ORDER BY Rating DESC) AS GenreRanking FROM userContent) select UserID, Genre AS favGenre, Rating from userGenreRank where GenreRanking = 1
"""

sqlDF = spark.sql(query)
sqlDF.show()

# save the output as parquet
output_path = "./output/advanced/content-analysis/showsWithHighestAverageRating.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)
