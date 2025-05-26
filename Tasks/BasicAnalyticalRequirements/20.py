from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, asc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# grouped df with show and average completion percent
aggDF = engagementDF.groupBy('ShowID').agg(avg('CompletionPercent').alias('AverageCompletionPercent'))

# ordered df 
orderedDF = aggDF.where(col('AverageCompletionPercent') < 60).orderBy(asc('AverageCompletionPercent'))

# show with lowest average completion rate
showDF = orderedDF.limit(1)
showDF.show()

# saving as parquet
output_path = './output/basic/showWithLowAverageCompletionPercent.parquet'
showDF.write.mode('overwrite').parquet(output_path)
