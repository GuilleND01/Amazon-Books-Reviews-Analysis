from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import *
import matplotlib.pyplot as plt

import time
start_time = time.time()

conf = SparkConf().setMaster('local[16]').setAppName('mostRatings')
conf.set("spark.sql.shuffle.partitions",300)
# mostrar el spark.master
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

input_file1 = "../dataset/reviews_grande.json" #valoraciones
input_file2 = "../dataset/metabooks.json" #libros

#numero de argumentos que se le pasan al programa
num_args = len(sys.argv) # para lista de categorias bucle con esto para coger todas y al comprobar en el filter con todas ellas.
cat = []
for y in range(1,num_args):
    cat.append(sys.argv[y])

#cat = ["Books", "Children's Books", "Arts Music & Photography"]

#leemos los ficheros

dfVal = spark.read.json(input_file1)
dfLib = spark.read.json(input_file2)


dfRead = dfVal.join(dfLib, dfLib.asin == dfVal.asin, 'inner') #unimos los dos dataframes por el asin
dfRead = dfRead.select('title', 'category', 'overall', "reviewerID") #seleccionamos las columnas que nos interesan

#filtramos por las categorias que nos interesan
dfFilter = dfRead.filter(func.array_contains(dfRead['category'], cat[0])) 
for x in range(1,len(cat)):
    dfFilter = dfFilter.filter(func.array_contains(dfRead['category'], cat[x]))


avg1 = dfFilter.groupBy('title').agg({'overall': 'avg', 'reviewerID':'count'}).orderBy(col("count(reviewerID)").desc()) #calculamos la media de cada libro y cuantas valoraciones tiene
avg1 = avg1.withColumnRenamed("avg(overall)", "rating") #renombramos la columna avg(overall) a avg
avg1 = avg1.withColumnRenamed("count(reviewerID)", "vals") #renombramos la columna count(reviewerID) a count

df_final = avg1.orderBy(col("rating").desc(), col("vals").desc()) #ordenamos por rating y por vals
df_final.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/bestBooksCat")

print("--- %s seconds ---" % (time.time() - start_time))
