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
	Libros de autores buscados segun el rango de precio
		1er argumento Rango Bajo PRECIO
		2do argumento Rango Alto PRECIO
		3er en adelante argumentos NOMBRE AUTOR
'''

##IMPORTANTE: 
#Ejemplo de entrada:	spark-submit .\booksPerAuthorAndPrice.py 10 40 John Ruskin

#numero de argumentos que se le pasan al programa
num_args = len(sys.argv) # para lista de categorias bucle con esto para coger todas y al comprobar en el filter con todas ellas.
cat = []
for y in range(1,num_args):
    cat.append(sys.argv[y])

#author = "['Fiona Cownie']"
precio1 = cat[0]
precio2 = cat[1]

precio1 = int(precio1)
precio2 = int(precio2)


#EL nombre del autor esta entre [''] en el dataframe asique se lo añado a la busqueda del usuario
author = cat[2]
for x in range(3,len(cat)):
	author = author + " " + cat[x]

#Leo los datos de los libros
input_file = "../dataset/meta_Books.json" #libros

df = spark.read.json(input_file)

#Selecciono las columnas donde el autor es el que busco
df = df.select(col('brand').alias("Autor"), col('title').alias("Titulo"), func.translate(func.col("price"), "$", "").alias("Precio"))\
		.where((df['brand'] == author) | df['brand'].contains(author))

#Filtro los resultados segú el rango de precios seleccionado
df = df.where(df["Precio"].between(precio1, precio2))

df = df.groupby(col("Autor"),col("Titulo")).agg(avg("Precio").alias("PreciO")).orderBy("PreciO", ascending=False)

df.show()

df.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/booksPerAuthorAndPrice.csv")
