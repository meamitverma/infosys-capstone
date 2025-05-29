from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# create temp views for user and content
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# average rating for movie with specific director
query = """
        SELECT TRIM(actor) AS Actor, AVG(Rating) AS AvgRating
        FROM (
                SELECT u.Rating, EXPLODE(SPLIT(c.actors, '\\\|')) AS Actor
                FROM content c
                JOIN users u ON u.ShowID = c.ShowID
            )
        GROUP BY TRIM(Actor)
"""
actorDF = spark.sql(query)
actorDF.show()


# save the output as parquet
output_path = "./output/advanced/content-genre/avgRatingWithActor.parquet"
actorDF.write.mode('overwrite').parquet(output_path)
