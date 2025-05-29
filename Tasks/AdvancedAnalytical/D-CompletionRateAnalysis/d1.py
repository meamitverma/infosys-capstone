from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')
usersDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
engagementDF.createOrReplaceTempView('engagements')
usersDF.createOrReplaceTempView('users')

# user with most completed movies
query = """
    WITH completedUsers AS (
        SELECT userid, completionpercent
        FROM engagements
        WHERE completionpercent = 100
    ),
    countUsers AS (
        SELECT userid, COUNT(*) AS userCount
        FROM completedUsers
        GROUP BY userid
    ),
    maxCountUsers AS (
        SELECT MAX(userCount) AS maxUserCount
        FROM countUsers
    )
    SELECT c.userid, c.userCount
    FROM countUsers c
    JOIN maxCountUsers m ON c.userCount = m.maxUserCount
"""
sqlDF = spark.sql(query)
sqlDF.show()


# save the output as parquet
output_path = "./output/advanced/D-CompletionRateAnalysis/usersWithMostCompleted.parquet"
sqlDF.write.mode('overwrite').parquet(output_path)