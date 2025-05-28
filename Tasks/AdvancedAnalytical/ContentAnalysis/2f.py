from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')
contentDF.createOrReplaceTempView('content')

# joined table
query = """
    SELECT *
    FROM users u
    JOIN content ON u.showid = c.showid
"""

joinedDF = spark.sql(query)
joinedDF.show()
