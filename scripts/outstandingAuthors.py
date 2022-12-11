from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import sys
from tkinter import *
from tkinter import messagebox as ms

conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

cat = sys.argv[1]

if ' and ' in cat:
    cat = cat.replace(' and ', ' & ')

cat1 = cat.replace('&', '&amp;')

input_file1 = "../dataset/reviews.json" #valoraciones
input_file2 = "../dataset/metabooks.json" #libros

# Lectura del archivo reviews
df1Read = spark.read.json(input_file2)
df2Read = spark.read.json(input_file1)

df2Read = df2Read.join(df1Read, df1Read.asin == df2Read.asin, 'inner')
df2Read = df2Read.select('overall', 'brand', 'category')

df2Gp = df2Read.filter((func.array_contains(df2Read['category'], cat) |
    func.array_contains(df2Read['category'], cat1)) & df2Read['brand'].isNotNull())

if df2Gp.isEmpty():
    ms.showerror("Error", 'La categoría introducida no existe o no tiene valoraciones')
    sys.exit()

df2Gp = df2Gp.groupBy('brand').agg({'overall': 'avg', 'brand':'count'})

df2Gp = df2Gp.toDF(*('Author', 'Overall Score', 'Num Reviews'))
df2Gp = df2Gp.withColumn('Overall Score', func.round(df2Gp['Overall Score'], 2))
df2Gp = df2Gp.orderBy(col('Overall Score').desc(), col('Num Reviews').desc())
df2Gp.show(truncate=False)

years = df2Gp.select(col('Author')).rdd.flatMap(lambda x: x).collect()
if len(years) > 20:
    years = years[0:19]


word_c = []
for x in years:
    aux = x.replace("Visit Amazon's ", "").replace(' Page', '').replace(' ', '')
    word_c.append(aux)
stra = ' '.join(word_c)
print(stra)

wordcloud = WordCloud(background_color = "white", max_words = 50).generate(stra)
plt.imshow(wordcloud)
plt.axis("off")
plt.title('Most outstanding authors of ' + cat)
plt.savefig('outstandingAuthorsof' + cat + '.png')
