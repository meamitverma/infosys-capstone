from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, col

spark = SparkSession.builder.getOrCreate()


# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# grouped df with show and average completion percent
aggDF = engagementDF.groupBy('ShowID').agg(avg('CompletionPercent').alias('AverageCompletionPercent'))

# show with lowest average completion rate
lowestCompletion = aggDF.agg(min('AverageCompletionRate').alias('LowestCompletion')).first()[0]
showDF = aggDF.filter(col('AverageCompletionRate') == lowestCompletion)
showDF.show()

# saving as parquet
output_path = './output/basic/showWithLowAverageCompletionPercent.parquet'
showDF.write.mode('overwrite').parquet(output_path)
