from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()


# load dataset to dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# create temp view
userDF.createOrReplaceTempView('users')

# creating dataframe
query = "SELECT * FROM users"
new_usersDF = spark.sql(query)
new_usersDF.show()

# save the output as parquet
output_path = "./output/advanced/A-content-genre-analysis/userData.parquet"
new_usersDF.write.mode('overwrite').parquet(output_path)

