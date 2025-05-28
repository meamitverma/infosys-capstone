from pyspark.sql import SparkSession
import shutil
import os

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# functions
def isValid(row, idxs):
    data = row.split(",")
    for idx in idxs:
        if (data[idx] == "?"): return False
    return True
        

def filterRDD(rdd, header, idxs=[0]):
    data = rdd.filter(lambda row : row != header) # remove header
    filtered_data = data.filter(lambda row : isValid(row, idxs)) # remove "?"
    distinct_data = filtered_data.distinct() # remove duplicates
    return distinct_data
    

# loading datasets from the hdfs into respective rdds
UserRDD = sc.textFile("./datasets/User_Data.csv")
ContentRDD = sc.textFile("./datasets/Content_Data.csv")
EngagementRDD = sc.textFile("./datasets/Engagement_Data.csv")

# headers
user_header = UserRDD.first()
content_header = ContentRDD.first()
engagement_header = EngagementRDD.first()

# filtered user
filtered_user = filterRDD(UserRDD, user_header)
filtered_content = filterRDD(ContentRDD, content_header)
filtered_engagement = filterRDD(EngagementRDD, engagement_header)

# fixing showids for userrdd after removing unwanted rows
filtered_user = filtered_user.map(lambda line : __import__('re').sub(r'\dM','M',line)).distinct()

# output directories
user_dir = './datasets/UserRDD.csv'
content_dir = './datasets/ContentRDD.csv'
engagement_dir = './datasets/EngagementRDD.csv'

# delete output directories if they exits
paths = [user_dir, content_dir, engagement_dir]
for path in paths:
    if os.path.exists(path):
        shutil.rmtree(path)

# saving the cleaned
filtered_user.saveAsTextFile(user_dir)
filtered_content.saveAsTextFile(content_dir)
filtered_engagement.saveAsTextFile(engagement_dir)


