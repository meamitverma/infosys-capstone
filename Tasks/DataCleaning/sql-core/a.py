from pyspark.sql import SparkSession

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()

user_df = spark.read.csv('./datasets/UserRDD.csv',inferSchema=True, header=False)
user_df.show()