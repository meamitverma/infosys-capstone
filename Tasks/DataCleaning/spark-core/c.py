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
        

def filter(rdd, header, idxs=[0]):

    data = rdd.filter(lambda line : line != header) # removes header
    filtered_data = data.filter(lambda row : isValid(row, idxs)) # removes row with "?" 
    distinct_data = filtered_data.distinct() # remove duplicate records

    return distinct_data

def addHeader(header, rdd):
    return sc.parallelize([header]).union(rdd)

# loading datasets from the hdfs into respective rdds
UserRDD = sc.textFile("./datasets/User_Data.csv")
ContentRDD = sc.textFile("./datasets/Content_Data.csv")
EngagementRDD = sc.textFile("./datasets/Engagement_Data.csv")

# headers
user_header = UserRDD.first()
content_header = ContentRDD.first()
engagement_header = EngagementRDD.first()

# filter rdds based on requirements
# indexes used for referring the userid,showid column number available in rdds
filtered_user = filter(UserRDD, user_header)
filtered_content = filter(ContentRDD, content_header)
filtered_engagement = filter(EngagementRDD, engagement_header, [0, 1])

# add headers
user_data = addHeader(user_header,filtered_user)
content_data = addHeader(content_header, filtered_content)
engagement_data = addHeader(engagement_header, filtered_engagement)

# saving the cleaned
user_data.saveAsTextFile('./Datasets/UserRDD')
content_data.saveAsTextFile('./Datasets/ContentRDD')
engagement_data.saveAsTextFile('./Datasets/EngagementRDD')
