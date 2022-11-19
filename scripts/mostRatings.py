from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as func
import sys

conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

''' Listado de los N libros con más valoraciones, su número y la media. Las valoraciones
son de usuarios diferentes, no considereamos dos valoraciones del mismo usuario '''

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
final_df.show(N,False)
