from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func
import matplotlib.pyplot as plt
import json
import pyspark.sql.types as ty
import sys

conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

def parse_array_from_string(x):
    res = json.loads(x)
    return res

retrieve_array = func.udf(parse_array_from_string, ty.ArrayType(ty.StringType()))

cat = sys.argv[1]

if ' and ' in cat:
    cat = cat.replace(' and ', ' & ')

# Lectura del archivo reviews
df1Read = spark.read.json('.\DATASET MAS GRANDE\DATASET MAS GRANDE\meta_Books.json')
df2Read = spark.read.json('.\DATASET MAS GRANDE\DATASET MAS GRANDE\Books_5.json')

df2Read = df2Read.join(df1Read, df1Read.asin == df2Read.asin, 'inner')
df2Read = df2Read.select('reviewTime', 'category', 'overall')

df2Read = df2Read.filter(func.array_contains(df2Read['category'], cat))

df2Read = df2Read.withColumn('reviewTime', df2Read['reviewTime'].cast('string'))

df2Read.select('overall', 'reviewTime')
df2Avg = df2Read.groupBy(df2Read['reviewTime'][-4:len(str(df2Read['reviewTime']))]).avg('overall')
df2Avg = df2Avg.toDF(*('Year', 'Overall Score'))
df2Avg = df2Avg.withColumn('Overall Score', func.round(df2Avg['Overall Score'], 2))
df2Avg = df2Avg.sort('Year')

fig, ax = plt.subplots()

# Paso a array los títulos y sus reviews
years = df2Avg.select(col('Year')).rdd.flatMap(lambda x: x).collect()
rating = df2Avg.select(col('Overall Score')).rdd.flatMap(lambda x: x).collect()

# Se pintan y unen los puntos
barGraphic = ax.scatter(years, rating)
plt.plot(years, rating)

ax.set_ylabel('Overall Score')
ax.set_title('Evolution of ' + cat)

plt.xticks(rotation=90)
plt.savefig(cat + '.png')