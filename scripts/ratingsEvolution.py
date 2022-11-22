from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func
import matplotlib.pyplot as plt

import sys

conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

cat = sys.argv[1]

# Lectura del archivo reviews
df1Read = spark.read.json(".\DATASET MAS GRANDE\DATASET MAS GRANDE\meta_Books.json")
df2Read = spark.read.json(".\DATASET MAS GRANDE\DATASET MAS GRANDE\Books_5.json")

df2Read = df2Read.join(df1Read, df1Read.asin == df2Read.asin, 'inner')
df2Read.select('category', 'overall', 'reviewTime')

df2Read = df2Read.withColumn('category', df2Read['category'].cast('string'))
df2Read = df2Read.filter(df2Read['category'].contains(cat))
df2Read = df2Read.select('reviewTime', 'title', 'overall')
df2Read = df2Read.withColumn('reviewTime', df2Read['reviewTime'].cast('string'))

df2Avg = df2Read.groupBy(df2Read['reviewTime'][-4:len(str(df2Read['reviewTime']))]).avg('overall')
df2Avg = df2Avg.toDF(*('Year', 'Overall Score'))
df2Avg = df2Avg.withColumn('Overall Score', func.round(df2Avg['Overall Score'], 2))
df2Avg = df2Avg.sort('Year')

fig, ax = plt.subplots()

# Paso a array los títulos y sus reviews
titles = df2Avg.select(col('Year')).rdd.flatMap(lambda x: x).collect()
rating = df2Avg.select(col('Overall Score')).rdd.flatMap(lambda x: x).collect()

# Pinto las barras y añado arriba el count exacto
barGraphic = ax.scatter(titles, rating)
plt.plot(titles, rating)

ax.set_ylabel('EVOLUTION OF REVIEWS')
ax.set_title(cat)

plt.xticks(rotation=90)
plt.savefig('evolution.png')