from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()


# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# filtered columns
query = "SELECT * FROM users"
filteredUserDF = spark.sql(query)
filteredUserDF.show()

# save the output as parquet
output_path = "./output/advanced/content-genre/userData.parquet"
filteredUserDF.write.mode('overwrite').parquet(output_path)

