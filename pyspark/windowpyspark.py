# Question:
#  Calculate previous day sale and next
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead

# Create a Spark session
spark = SparkSession.builder \
 .appName("CalculateSales") \
 .getOrCreate()

# Define sample data
data = [
 ("TV", "2016-11-27", 800),
 ("TV", "2016-11-30", 900),
 ("TV", "2016-12-29", 500),
 ("FRIDGE", "2016-10-11", 760),
 ("FRIDGE", "2016-10-13", 400)
]

# Create DataFrame from sample data
df = spark.createDataFrame(data, ["PRODUCT", "SALE_DATE", "AMOUNT"])
df = df.withColumn("SALE_DATE", col("SALE_DATE").cast("date"))

# Define window specification
window_spec = Window.partitionBy("PRODUCT").orderBy("SALE_DATE")

# Challenge : 𝑭𝒊𝒏𝒅 𝒕𝒉𝒆 𝒔𝒕𝒂𝒓𝒕 𝒂𝒏𝒅 𝒆𝒏𝒅 𝒍𝒐𝒄𝒂𝒕𝒊𝒐𝒏 𝒇𝒐𝒓 𝒆𝒂𝒄𝒉 𝒄𝒖𝒔𝒕𝒐𝒎𝒆𝒓.

data = [ ('c1', 'New York', 'Lima'),
 ('c1', 'London', 'New York'),
 ('c1', 'Lima', 'Sao Paulo'),
 ('c1', 'Sao Paulo', 'New Delhi'),
 ('c2', 'Mumbai', 'Hyderabad'),
 ('c2', 'Surat', 'Pune'),
 ('c2', 'Hyderabad', 'Surat'),
 ('c3', 'Kochi', 'Kurnool'),
 ('c3', 'Lucknow', 'Agra'),
 ('c3', 'Agra', 'Jaipur'),
 ('c3', 'Jaipur', 'Kochi')]

schema = "customer string , start_location string , end_location string"
df = spark.createDataFrame(data = data , schema = schema)

start_df=df.alias("start").join(df.alias("end"),f.col("start.start_location")==f.col("end.end_location"),"left").select("start.customer","start.start_location","end.end_location").filter(f.col("end_location").isNull())

end_df=df.alias("start").join(df.alias("end"),f.col("start.end_location")==f.col("end.start_location"),"left").select("start.customer","start.end_location","end.start_location").filter(f.col("start_location").isNull())

final_result=start_df.alias("start").join(end_df.alias("end"),f.col("start.customer")==f.col("end.customer"),"inner").select("start.customer","start.start_location","end.end_location")


# Which user flagged the most distinct videos that ended up approved by YouTube? Output, in one column, their full name or names in case of a tie. In the user's full name, include a space between the first and the last name.

# Tables: user_flags, flag_review
# Import your libraries
###
+--------------+-------------+-----------+-------+
|user_firstname|user_lastname|   video_id|flag_id|
+--------------+-------------+-----------+-------+
|       Richard|       Hasson|y6120QOlsfU| 0cazx3|
|          Mark|          May|Ct6BUPvE2sM| 1cn76u|
|          Gina|       Korman|dQw4w9WgXcQ| 1i43zk|
|          Mark|          May|Ct6BUPvE2sM| 1n0vef|
|          Mark|          May|jNQXAC9IVRw| 1sv6ib|
|          Gina|       Korman|dQw4w9WgXcQ| 20xekb|
|          Mark|          May|5qap5aO4i9A| 4cvwuv|
|        Daniel|         Bell|5qap5aO4i9A| 4sd6dv|
|       Richard|       Hasson|y6120QOlsfU| 6jjkvn|
|       Pauline|        Wilks|jNQXAC9IVRw| 7ks264|
|      Courtney|         null|dQw4w9WgXcQ|   null|
|         Helen|        Hearn|dQw4w9WgXcQ| 8946nx|
|          Mark|      Johnson|y6120QOlsfU| 8wwg0l|
|       Richard|       Hasson|dQw4w9WgXcQ| arydfd|
|          Gina|       Korman|       null|   null|
|          Mark|      Johnson|y6120QOlsfU| bl40qw|
|       Richard|       Hasson|dQw4w9WgXcQ| ehn1pt|
|          null|        Lopez|dQw4w9WgXcQ| hucyzx|
|          Greg|         null|5qap5aO4i9A|   null|
|       Pauline|        Wilks|jNQXAC9IVRw| i2l3oo|
+--------------+-------------+-----------+-------+


+-------+--------------+-------------+----------------+
|flag_id|reviewed_by_yt|reviewed_date|reviewed_outcome|
+-------+--------------+-------------+----------------+
| 0cazx3|         false|         null|            null|
| 1cn76u|          true|   2022-03-15|         REMOVED|
| 1i43zk|          true|   2022-03-15|         REMOVED|
| 1n0vef|          true|   2022-03-15|         REMOVED|
| 1sv6ib|          true|   2022-03-15|        APPROVED|
| 20xekb|          true|   2022-03-17|         REMOVED|
| 4cvwuv|          true|   2022-03-15|        APPROVED|
| 4l1tk7|         false|         null|            null|
| 4sd6dv|          true|   2022-03-14|         REMOVED|
| 6jjkvn|          true|   2022-03-16|        APPROVED|
| 7ks264|          true|   2022-03-15|        APPROVED|
| 8946nx|         false|         null|            null|
| 8wwg0l|         false|         null|            null|
| arydfd|          true|   2022-03-15|        APPROVED|
| bl40qw|          true|   2022-03-16|         REMOVED|
| ehn1pt|          true|   2022-03-18|        APPROVED|
| hucyzx|         false|         null|            null|
| i2l3oo|          true|   2022-03-17|         REMOVED|
| i6336w|         false|         null|            null|
| iey5vi|         false|         null|            null|
+-------+--------------+-------------+----------------+
####

import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat,lit,countDistinct,dense_rank,desc

# Assuming user_flags and flag_review dataframes are already defined

df = user_flags.join(flag_review, on=(user_flags.flag_id == flag_review.flag_id), how='inner')

df = df.filter(col("reviewed_outcome") == 'APPROVED')

df = df.withColumn("username", concat(col("user_firstname"), lit(" "), col("user_lastname")))

df=df.groupby(col("username")).agg(
    countDistinct(col("video_id")).alias("unique_videoid")
    )

df = df.withColumn("rnk",dense_rank().over(Window.orderBy(col("unique_videoid").desc())))

df=df.filter(col("rnk")==1)\
        .select(col("username"))
df.toPandas()

# Create DataFrame Code : 
# ====================
data = [ ('c1', 'New York', 'Lima'),
    ('c1', 'London', 'New York'),
    ('c1', 'Lima', 'Sao Paulo'),
    ('c1', 'Sao Paulo', 'New Delhi'),
    ('c2', 'Mumbai', 'Hyderabad'),
    ('c2', 'Surat', 'Pune'),
    ('c2', 'Hyderabad', 'Surat'),
    ('c3', 'Kochi', 'Kurnool'),
    ('c3', 'Lucknow', 'Agra'),
    ('c3', 'Agra', 'Jaipur'),
    ('c3', 'Jaipur', 'Kochi')]

schema = "customer string , start_location string , end_location string"
df = spark.createDataFrame(data = data , schema = schema)
df.show()
df1=df.select("customer","start_location")
df2=df.select("customer","end_location")
df3=df1.union(df2).groupBy("customer","start_location").agg(count("start_location").alias("count")).filter("count==1")
df3.alias("a").join(df3.alias("b"),["customer"],"inner").filter("a.start_location<b.start_location").selectExpr("customer","a.start_location","b.start_location as end_location").display()