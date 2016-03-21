"""Tweet Analysis"""

# using PySpark on EC2 Spark cluster, SparkContext and SQLContext are loaded
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark import SparkContext

# setting up SparkContext
sc = SparkContext(appName="Tweet Analysis")

sqlContext = SQLContext(sc)

# reading the json-format tweet file for further analysis
df = sqlContext.read.json("tweets/cleaned_2012_01_0.json")

# Temporary Table
df.registerTempTable("df")


# only keep tweets in English
lang_en_tweet = sqlContext.sql("SELECT * FROM df WHERE lang = 'en'")
lang_en_tweet.registerTempTable("lang_en_tweet")

# keep tweets where Gooogle is mentioned and save the query as text file
google_tweet = sqlCOntext.sql("SELECT * FROM lang_en_tweet WHERE text like '%Google%' or text like '%google%'")
google_tweet.write.text("/user/root/google_tweet.txt")

# exported text file
logFile = "/user/root/google_tweet.txt" 
logData = sc.textFile(logFile).cache()

numAndroid = logData.filter(lambda s: 'Android' in s).count()
numChrome = logData.filter(lambda s: 'Chrome' in s).count()

print("Tweets with Android mentioned: %i, tweets with Chrome mentioned: %i" % (numAndroid, numChrome))
