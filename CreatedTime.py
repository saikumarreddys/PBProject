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


sqldf = spark.sql("SELECT substring(createdat,length(createdat)-3,4) year,count(*) as Count  FROM hashtags \
                 group by substring(createdat,length(createdat)-3,4) order by count desc limit 10")
sqldf.show(150)

#sqldf.toPandas().to_csv('topUsers.csv')

