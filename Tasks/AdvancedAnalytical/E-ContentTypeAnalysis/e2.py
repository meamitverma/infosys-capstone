from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# create temp view
contentDF.createOrReplaceTempView('content')
engagementDF.createOrReplaceTempView('engagement')

# higher engagement based on genre
query1 = """
    SELECT genre, AVG(completionPercent) AS averageCompletionPercent, AVG(DATEDIFF(TO_DATE(playbackStopped), TO_DATE(playbackStarted))) AS averageWatchTime
    FROM content c
    JOIN engagement e ON c.showid = e.showid
    GROUP BY genre
"""
sql1DF = spark.sql(query1)
sql1DF.show()

# higher engagement based on actor
query2 = """
    SELECT actor, AVG(completionPercent) AS averageCompletionPercent, AVG(DATEDIFF(TO_DATE(playbackStopped,'yyyy-MM-dd'), TO_DATE(playbackStarted,'yyyy-MM-dd'))) AS averageWatchTime
    FROM (
        SELECT playbackStarted, playbackStopped, completionPercent, EXPLODE(SPLIT(actors, '\\\\|')) AS actor
        FROM engagement e
        JOIN content c ON e.showid = c.showid
    )
    GROUP BY actor
"""
sql2DF = spark.sql(query2)
sql2DF.show()

# save the output as parquet
output_path = "./output/advanced/E-ContentTypeAnalysis/"
sql1DF.write.mode('overwrite').parquet(output_path + '')
sql2DF.write.mode('overwrite').parquet(output_path)