from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# creat sparksession
spark = SparkSession.builder.getOrCreate()

# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# join using broadcast
optimizedDF = contentDF.join(broadcast(userDF), 'ShowID')

# create temp view for joineddf
optimizedDF.createOrReplaceTempView("contentWatch")

# query
query = """
    SELECT * FROM contentWatch
"""
joinedDF = spark.sql(query)
joinedDF.show()


# save the output as parquet
output_path = "./output/advanced/content-genre/broadcastJoinContentWatch.parquet"
joinedDF.write.mode('overwrite').parquet(output_path)
