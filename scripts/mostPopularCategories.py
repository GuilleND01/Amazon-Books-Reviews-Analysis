from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import *
import matplotlib.pyplot as plt

#Categorías más populares(por numero de reviews)

conf = SparkConf().setMaster('local[*]').setAppName('mostRatings')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

input_file1 = "../dataset/reviews_grande.json" #valoraciones
input_file2 = "../dataset/metabooks.json" #libros

dfVal = spark.read.json(input_file1)
dfLib = spark.read.json(input_file2)

dfRead = dfVal.join(dfLib, dfLib.asin == dfVal.asin, 'inner') #unimos los dos dataframes por el asin
dfRead = dfRead.select('category', "reviewerID") #seleccionamos las columnas que nos interesan
#dfread = dfRead.withColumn("category", explode("category")) #descomponemos la columna category en varias filas
dfCount = dfRead.groupBy('category').agg({'reviewerID':'count'}).orderBy(col("count(reviewerID)").desc()) #calculamos el numero de reviews de cada categoria
dfCount = dfCount.withColumnRenamed("count(reviewerID)", "vals") #renombramos la columna count(reviewerID) a count
dfCount = dfCount.filter(func.size(dfCount.category) > 0) #filtramos las categorias que no tienen valor
dfCount = dfCount.withColumn('category', dfCount['category'].cast('string'))

#reemplazar valores de un string de la columna category
dfCount = dfCount.withColumn('category', regexp_replace('category', '&amp;', '&'))
dfCount = dfCount.groupBy('category').sum('vals')
dfCount = dfCount.withColumnRenamed("sum(vals)", "vals")
dfCount = dfCount.orderBy(col("vals").desc()).toDF('category', 'vals')

dfCount.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/mostPopularCategories")

