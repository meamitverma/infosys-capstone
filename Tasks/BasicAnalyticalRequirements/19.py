from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.getOrCreate()


# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# grouped df with show and average completion percent
aggDF = engagementDF.groupBy('ShowID').agg(avg('CompletionPercent').alias('AverageCompletionPercent'))

# filter records with less than 60%
filteredDF = aggDF.where(col('AverageCompletionPercent') < 60)
filteredDF.show()

# saving as parquet
output_path = './output/basic/userWithShowDuration.parquet'
# userDF.write.mode('overwrite').parquet(output_path)
