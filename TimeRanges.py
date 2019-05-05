# Python version 2.7.6
# Import the datetime and pytz modules.

import datetime
import pytz
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys,tweepy,csv,re
from textblob import TextBlob
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd

datetime_obj = datetime.datetime.strptime('Sun Oct 12 10:53:51 +0000 2014', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Sun Oct 12 10:53:51 +0000 2014','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))

spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("hashtags1.txt")
df.createOrReplaceTempView("hashtags")


sqldf = spark.sql("SELECT timestamp as Latest FROM hashtags  order by Latest desc limit 1 ")

sqldf1 = spark.sql("select timestamp as Earliest from hashtags order by Earliest asc limit 1")

sqldf.show(150)
sqldf1.show(150)

#sqldf.toPandas().to_csv('topUsers.csv')

