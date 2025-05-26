from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# paired with showid and avg of completion percent
aggDF = engagementDF.groupBy('ShowID').agg(avg('CompletionPercent').alias('AverageCompletion'))

# show with maximum completion rate
showDF = aggDF.orderBy(desc('AverageCompletion')).limit(1)
showDF.show()

# saving as parquet
output_path = './output/basic/showWithMaximumCompletionPercent.parquet'
showDF.write.mode('overwrite').parquet(output_path)
