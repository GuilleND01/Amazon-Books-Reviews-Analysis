from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import avg, when, collect_list
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as func
#import Matplotlib as plt
import sys
import re

conf = SparkConf().setMaster('local[*]').setAppName('bookPerAuthorAndPrice')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

'''
	Precio de los libros de una categoria a lo largo de un tiempo 
'''

##IMPORTANTE: 
#Ejemplo de entrada:	spark-submit .\booksPriceEvolution.py 10-20-2018 10-20-2019 arts-and-crafts

#numero de argumentos que se le pasan al programa
num_args = len(sys.argv) # para lista de categorias bucle con esto para coger todas y al comprobar en el filter con todas ellas.
cat = []
for y in range(1,num_args):
    cat.append(sys.argv[y])

#author = "['Fiona Cownie']"
#Fecha en formato dia-mes-año
fecha1 = cat[0]
fecha2 = cat[1]

categoria = cat[2]
for x in range(3,len(cat)):
	categoria = categoria + " " + cat[x]

#Leo los datos de los libros
input_file = "../dataset/meta_Books.json" #libros

df = spark.read.json(input_file)

#Selecciono las columnas de fecha, title y precio
#Filtro los libros por categoria
#Agrupo los libros por año y calxulo el precio medio de estos
#Ordeno los resultados por año

#Selecciono las columnas de fecha, title y precio
df = df.select('title'.alias("Título"), func.translate(func.col("price"), "$", "").alias("Precio")
                , func.col("categories").alias("Categoría")
                , func.col("date").alias("Fecha"))

df = df.filter(df["Categoría"].contains(categoria))

#Sepero la fecha en dia, mes y año
df = df.withColumn("Fecha", func.split(func.col("Fecha"), "-"))

#Selecciono el año
df = df.withColumn("Fecha", func.col("Fecha")[2])

#Agrupo los libros por año y calculo el precio medio de estos
df = df.groupBy("Fecha").agg(avg("Precio").alias("Precio medio"))

#Ordeno los resultados por año
df = df.orderBy("Fecha", ascending = True)

df.show()

df.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/booksPriceEvolution.csv")
