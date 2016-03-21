"""Tweet Analysis"""

# using PySpark on EC2 Spark cluster, SparkContext and SQLContext are loaded
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark import SparkContext


sc = SparkContext(appName="Tweet Analysis")

sqlContext = SQLContext(sc)

# reading the json-format tweet file for further analysis
df = sqlContext.read.json("tweets/cleaned_2012_01_0.json")

