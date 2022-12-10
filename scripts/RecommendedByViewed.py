from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.functions import collect_list
from pyspark.sql.types import *
import matplotlib.pyplot as plt

conf = SparkConf().setMaster('local[*]').setAppName('recommendedByViewed')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

libros = "../dataset/metabooks.json" #libros
valoraciones = "../dataset/reviews_grande.json" #valoraciones

#titulo del libro a buscar pasado por argumento
titulo = sys.argv[1]

#Leemos los libros
dfLib = spark.read.json(libros)
#Seleccionamos columnas
dfLib = dfLib.select('asin', 'title', 'category', 'price', 'brand', 'description') 
dfLib.show()
#filtramos por el titulo dado
dfLib = dfLib.filter(col("title").contains(titulo))

#Si hay varios libros con el mismo libro cogemos solo el primero
id = dfLib.agg(collect_list('asin')).collect()[0][0][0]
#Cogemos otro df con los libros
dfRec = spark.read.json(libros)

#Seleccionamos columnas 
dfRec = dfRec.select('asin', 'title', 'category','price', 'brand', 'description', 'also_view') 
#filtramos por libros que tambien son comprados por usuarios que commpran el libro que hemos buscado anteriormente
dfRec = dfRec.filter(func.array_contains(dfRec['also_view'], id)) 
dfRec.show()
#Seleccionamos columnas solo que esta vez solo con las que nos interesa mostrar y mostramos
dfRec = dfRec.select('asin', 'title', 'category','price', 'brand', 'description')

#Leemos las valoraciones
dfVal = spark.read.json(valoraciones)

#Juntamos los libros sacados con sus reviews
dfRead = dfVal.join(dfRec, dfRec.asin == dfVal.asin, 'right')
dfRead = dfRead.select('title', 'category', 'overall', "reviewerID")


#Sacamos media de los reviews y el num de reviews
dfavg = dfRead.groupBy('title').agg({'overall': 'avg', 'reviewerID':'count'}).orderBy(col("count(reviewerID)").desc()) 
dfavg = dfavg.withColumnRenamed("avg(overall)", "rating") 
dfavg = dfavg.withColumnRenamed("count(reviewerID)", "vals")

#Ordenamos por sus rating y vals
dfFinal= dfavg.orderBy(col("rating").desc(), col("vals").desc()) 
dfFinal.show()
#Guardamos en un fichero
dfFinal.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/recommended.csv")
