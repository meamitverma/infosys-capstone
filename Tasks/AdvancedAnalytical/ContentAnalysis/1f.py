from pyspark.sql import SparkSession

# creat sparksession
spark = SparkSession.builder.getOrCreate()


# load dataset to dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
userDF = spark.read.parquet('./datasets/UsersWatchData.parquet')

# save the output as parquet
output_path = "./output/advanced/content-analysis/"
userDF.write.mode('overwrite').parquet(output_path + 'userdf.parquet')
contentDF.write.mode('overwrite').parquet(output_path + 'contentdf.parquet')

