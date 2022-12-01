from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.functions import collect_list
from pyspark.sql.types import *
import matplotlib.pyplot as plt

conf = SparkConf().setMaster('local[*]').setAppName('recommended')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

libros = "./dataset/meta_Books.json"

#titulo del libro a buscar pasado por argumento
titulo = sys.argv[1]

#Leemos los libros
dfLib = spark.read.json(libros)

#Seleccionamos columnas
dfLib = dfLib.select('asin', 'title', 'categories', 'price', 'brand', 'description') 

#filtramos por el titulo dado
dfLib = dfLib.filter(col("title").contains(titulo))

#Si hay varios libros con el mismo libro cogemos solo el primero
cat = []
cat = dfLib.agg(collect_list('asin')).collect()
#cat = dfLib.col('asin')


#Cogemos otro df con los libros
dfRec = spark.read.json(libros)

#Seleccionamos columnas 
dfRec = dfRec.select('asin', 'title', 'categories','price', 'brand', 'description', 'also_buy') 

#filtramos por libros que tambien son comprados por usuarios que commpran el libro que hemos buscado anteriormente
dfRec = dfRec.filter(func.array_contains(dfRec['also_buy'], cat[0])) 

#Seleccionamos columnas solo que esta vez solo con las que nos interesa mostrar y mostramos
dfRec = dfRec.select('asin', 'title', 'categories','price', 'brand', 'description')
dfRec.show()