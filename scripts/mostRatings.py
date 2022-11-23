from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func
import sys
import matplotlib.pyplot as plt 


conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

''' Listado de los N libros con más valoraciones, su número y la media.  '''

# spark-submit mostRatings.py N
N = int(sys.argv[1])

# Lectura del archivo reviews
dfRead = spark.read.json("../dataset/reviews.json")

# Borro aquellas columnas que no voy a necesitar:

# 1.Selecciono las que quiero
final_colums = ["asin", "overall", "reviewerID"]
# 2.Selecciono todas las del DS
colNames = dfRead.schema.names
# 3.Borro todas aquellas que no estén en final_colums
dfRead = dfRead.drop(*set(colNames).symmetric_difference(set(final_colums)))

# Para ver que se han borrado: dfRead.printSchema()

# Agrupamos por el ID del producto y lo paso a una lista
dataRead = dfRead.groupBy("asin").agg({'overall': 'avg', 'reviewerID':'count'}).orderBy(col("count(reviewerID)").desc())

# Creo un nuevo DF a partir de los N primeros resultados
DFreadN = spark.createDataFrame(data = dataRead.take(N), schema=["asin", "Number of Reviews", "Average Rating"])

# Lectura del archivo metabooks.json para sacar los títulos de los N libros
dfReadBooks = spark.read.json("../dataset/metabooks.json")

# Me quedo con las columnas que me interesan
meta_columns = ["asin", "title"]
booksColNames = dfReadBooks.schema.names
dfReadBooks =  dfReadBooks.drop(*set(booksColNames).symmetric_difference(set(meta_columns)))\
    .withColumnRenamed("title", "Title")

# Join de los dos DF según id del producto. Vuelvo a reordenar, porque el join no mantiene el orden
final_df = dfReadBooks.join(DFreadN, DFreadN.asin == dfReadBooks.asin, "right").drop("asin")\
    .withColumn("Average Rating", func.round(col("Average Rating"),2)).orderBy(col("Number of Reviews").desc())

#final_df.show(N,False)

final_df.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/" + str(N) + "_mostRatings")

# Procesamiento gráfico
fig, ax = plt.subplots()

# Paso a array los títulos y sus reviews
titles = final_df.select(col("Title")).rdd.flatMap(lambda x: x).collect()
counts =  final_df.select(col("Number of Reviews")).rdd.flatMap(lambda x: x).collect()
rating = final_df.select(col("Average Rating")).rdd.flatMap(lambda x: x).collect()

# Pinto las barras y añado arriba el count exacto
barGraphic = ax.bar(titles, counts)
ax.bar_label(barGraphic, counts)

# Ahora añado el average rating dentro de la barra
i = -1
for bar in ax.patches:
    i += 1
    # Se centra según el ancho y alto de cada barra
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + bar.get_height() / 2,
    rating[i], ha='center', color='w', weight='bold', size=10)

ax.set_ylabel('NUMBER OF REVIEWS')
ax.set_title('THE ' + str(N) + ' BOOKS WITH THE MOST RATINGS')

plt.savefig('../results/' + str(N) + '_mostRatings.png')
