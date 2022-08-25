import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, min, max
from datetime import date
import pandas as pd
from pyspark.sql import functions as f

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0") \
      .getOrCreate()

def read_users_data():
    df = spark.read \
        .format("xml") \
        .options(rowTag='row')\
        .load(os.getcwd() + '/data' + '/Posts.xml')

    # print(df.show())
    return df

def total_posts():
    df = read_users_data()
    print("Total number of posts are ",df.count())

def last_post():
    df = read_users_data()
    # print(df.dtypes)
    df = df.filter(df._CommentCount >= 1)
    # print(df.show())
    df.withColumn("_CreationDate", unix_timestamp("_CreationDate")).agg(
        from_unixtime(max("_CreationDate")).alias("max_ts")
    ).show()
    # print(df.show())

def last_month_posts():
    df = read_users_data()
    curr_date = datetime.datetime.now()
    pre_month = curr_date - pd.DateOffset(months=3)
    dates = (pre_month, datetime.date.today())
    print(df.where(F.col('_CreationDate').between(*dates)).count())

def average_comments():
    df = spark.read \
        .format("xml") \
        .options(rowTag='row') \
        .load(os.getcwd() + '/data' + '/Comments.xml')
    df = (df.withColumn('year_month', F.date_format(F.col('_CreationDate'),\
        '1/M/yyyy'))\
     .groupBy('year_month')\
     .agg(F.count(F.col('year_month')))\
     )
    df.agg({'count(year_month)':"avg"}).show()

def badges():
    df = spark.read \
        .format("xml") \
        .options(rowTag='row') \
        .load(os.getcwd() + '/data' + '/Badges.xml')

    df = df.withColumn('is_critic', F.when(F.col('_Name') == "Critic", 1)\
                  .otherwise(0))

    df = df.withColumn('is_editor', F.when(F.col('_Name') == "Editor", 1) \
                  .otherwise(0))
    df.show()


if __name__ == '__main__':
    # total_posts()
    # last_post()
    # last_month_posts()
    # average_comments()
    badges()