from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# fav genre
query = """
    WITH genreRank AS (
        SELECT u.userid, c.genre, CAST(u.rating AS INT) AS rating, RANK() OVER (PARTITION BY u.userid ORDER BY u.rating DESC) AS genreRanking
        FROM users u
        JOIN content c ON u.showid = c.showid
    )
    SELECT userid, genre, rating
    FROM genreRank
    WHERE genreRanking = '1'
    ORDER BY userid
"""
favGenreDF = spark.sql(query)
favGenreDF.show()

# save the output as parquet
output_path = "./output/advanced/F-ContentAnalysis/usersFavoriteGenre.parquet"
favGenreDF.write.mode('overwrite').parquet(output_path)
