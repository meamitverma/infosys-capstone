from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')


# movies from fav genre that users have not watched
query = """
    WITH genreRank AS (
        SELECT u.userid, c.genre, u.showid, CAST(u.rating AS INT) AS rating, RANK() OVER (PARTITION BY u.userid ORDER BY u.rating DESC) AS genreRanking
        FROM users u
        JOIN content c ON u.showid = c.showid
    )
    SELECT ug.userid, ug.genre AS favGenre, c.showid AS suggestedShow
    FROM content c
    JOIN genreRank ug ON c.genre = ug.genre AND genreRanking = 1 AND c.showid != ug.showid
    ORDER BY ug.userid, favGenre, suggestedShow
"""
moviesDF = spark.sql(query)
moviesDF.show()


# save the output as parquet
output_path = "./output/advanced/F-ContentAnalysis/showsWithGenreNotWatched.parquet"
moviesDF.write.mode('overwrite').parquet(output_path)
