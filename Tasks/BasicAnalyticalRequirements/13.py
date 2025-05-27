from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max

spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# filtered dataframes
filtered_user = userDF.select('UserID', 'ShowID', 'Rating')
filtered_content = contentDF.select('ShowID','Genre')

# joined dataframe
joinedDF = filtered_user.join(filtered_content, on="ShowID", how="inner")

# maximum rating
aggMaxDF = joinedDF.groupBy('Genre').agg(max('Rating').alias('MaxRating'))
maxRating = aggMaxDF.agg(max('MaxRating').alias('MaxRating')).first()[0]
maxDF = aggMaxDF.filter(col('MaxRating') == maxRating)
maxDF.show()

# highest average rating
aggHighestAvgDF = joinedDF.groupBy('Genre').agg(avg('Rating').alias('HighestAvgRating'))
highestAvgRating = aggHighestAvgDF.agg(max('HighestAvgRating').alias('HighestAvgRating')).first()[0]
highestAvgDF = aggHighestAvgDF.filter(col('HighestAvgRating') == highestAvgRating)
highestAvgDF.show()


# save as parquet
output_path = './output/basic/'
maxDF.write.mode('overwrite').parquet(output_path + 'maxRatingPerGenre.parquet')
highestAvgDF.write.mode('overwrite').parquet(output_path + 'highestAvgRatingPerGenre.parquet')
