from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, max

spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# filtered dataframes
filtered_user = userDF.select('UserID','Rating')
filtered_content = contentDF.select('ShowID','Genre')
filtered_engagement = engagementDF.select('UserID', 'ShowID')

# joined dataframe
userEngagementContent = filtered_user.join(filtered_engagement, on="UserID", how="inner")
joinedDF = userEngagementContent.join(filtered_content, on="ShowID", how="inner")

# maximum rating
maxDF = joinedDF.groupBy('Genre').agg(max('Rating').alias('MaxRating')).orderBy(desc('MaxRating')).limit(1)
maxDF.show()

# highest average rating
maxAvgDF = joinedDF.groupBy('Genre').agg(avg('Rating').alias('AverageRating')).orderBy(desc('AverageRating')).limit(1)
maxAvgDF.show()
