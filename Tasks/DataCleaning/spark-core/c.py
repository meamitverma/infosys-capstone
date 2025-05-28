from pyspark.sql import SparkSession

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

# saving the cleaned
filtered_user.saveAsTextFile('./datasets/UserRDD.csv')
filtered_content.saveAsTextFile('./datasets/ContentRDD.csv')
filtered_engagement.saveAsTextFile('./datasets/EngagementRDD.csv')

