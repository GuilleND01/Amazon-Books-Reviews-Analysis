from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import *
import matplotlib.pyplot as plt

conf = SparkConf().setMaster('local[*]').setAppName('searchBookInfo')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

libros = "./dataset/meta_Books.json"

#titulo del libro a buscar pasado por argumento
titulo = sys.argv[1]

#Leemos los libros
dfLib = spark.read.json(libros)

#Seleccionamos columnas
dfLib = dfLib.select('title', 'categories', 'price', 'brand', 'description') 

#filtramos por el titulo dado y mostramos el libro
dfLib = dfLib.filter(col("title").contains(titulo))
dfLib.show()
