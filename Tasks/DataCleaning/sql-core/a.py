from pyspark.sql import SparkSession

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()

user_df = spark.read.csv('./datasets/UserRDD', schema="UserID",inferSchema=True, header=False)
user_df.show()