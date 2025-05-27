from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# paired with showid and avg of completion percent
aggDF = engagementDF.groupBy('ShowID').agg(avg('CompletionPercent').alias('AverageCompletion'))

# show with maximum completion rate
maxCompletionRate = aggDF.agg(max('AverageCompletion').alias('MaxAvgCompletionRate')).first()[0]

showDF = aggDF.filter(col('AverageCompletion') == maxCompletionRate)
showDF.show()

# saving as parquet
output_path = './output/basic/showWithMaximumCompletionPercent.parquet'
showDF.write.mode('overwrite').parquet(output_path)
