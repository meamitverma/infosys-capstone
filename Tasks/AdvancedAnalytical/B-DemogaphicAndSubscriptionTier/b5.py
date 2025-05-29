from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col,count

# create spark session
spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# shows popular among standard users
query = """
    WITH standardUsers AS (
        SELECT *
        FROM users
        WHERE subscription = 'Standard'
    ),
    userCounts AS (
        SELECT showid, COUNT(showid) AS usersCount
        FROM standardUsers
        GROUP BY showid
    ),
    maxCount AS (
        SELECT MAX(usersCount) AS maxCount
        FROM userCounts
    )
    SELECT s.showid, s.usersCount
    FROM userCounts s
    JOIN maxCount m
    ON s.usersCount = m.maxCount
"""
showsDF = spark.sql(query)
showsDF.show()


# save the output as parquet
output_path = "./output/advanced/B-demographic-and-subscription-tier/popularShowsAmongStandard.parquet"
showsDF.write.mode('overwrite').parquet(output_path)