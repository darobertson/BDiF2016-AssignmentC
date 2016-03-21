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

# keep tweets where Gooogle is mentioned
google_tweet = sqlCOntext.sql("SELECT * FROM lang_en_tweet WHERE text like '%Google%' or text like '%google%'")


