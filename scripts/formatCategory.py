from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func
import sys
import matplotlib.pyplot as plt 


conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

''' Para una categoría, se muestra en un gráfico circular su top 5 formatos de libros según 
el número de valoraciones que los libros de esta categoría hayan recibido en ese formato. El resto
de formatos se podrán visualizar en un csv '''

cat = sys.argv[1]
cat1 = cat.replace('&', '&amp;')

# Lectura del archivo reviews
dfReviews = spark.read.json("../dataset/reviews.json")

# Lectura del archivo metabooks
dfReadBooks = spark.read.json("../dataset/metabooks.json")

# Se unen los dataframes por el identificador del libro
dfJoin = dfReviews.join(dfReadBooks, dfReadBooks.asin == dfReviews.asin)
dfJoin = dfJoin.select('category', 'style')

# Quito los estilos que no voy a usar
dfJoin = dfJoin.select(col('style.Format:'), col('category')).toDF('format', 'category')

# Se filtran según la categoría introducida
dfFilter = dfJoin.filter(func.array_contains(dfJoin['category'], cat) | 
    func.array_contains(dfJoin['category'], cat1))

if dfFilter.isEmpty():
    sys.exit()
    
# Se hace un group-count por tipo de formato
dfGroup = dfFilter.groupBy('format').count()

dfGroup = dfGroup.orderBy(col("count").desc())

# Ignoramos aquellos valoraciones del libro que no tengan formato especificado
dfGroup = dfGroup.select("*").where(col("format") != 'null')

# Parte gráfica
labels = dfGroup.select(col("format")).limit(5).rdd.flatMap(lambda x: x).collect()
sizes = dfGroup.select(col("count")).rdd.flatMap(lambda x: x).collect()

# Se seleccionan los 5 primeros formatos más populares y el resto se agrupan en una sección "All other categories"
allSizes = sizes[5:]
sizes = sizes[0:5]

allSize = sum(allSizes)

sizes.append(allSize)
labels.append(" All other formats")

fig1, ax1 = plt.subplots()
ax1.pie(sizes, shadow=False, startangle=90)

i = 0
sumAllSizes = sum(sizes)
for label in labels:
    labels[i] = str(label + ' (' + str(round(sizes[i] / sumAllSizes * 100, 2)) + '%)')
    i = i + 1

plt.legend(labels,loc="best")

ax1.axis('equal')  
plt.title('Format popularity of '+ cat)

dfGroup.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/" + str(cat) + "_formatCategory")
plt.savefig('../results/' + str(cat) + '_formatCategory.png')
