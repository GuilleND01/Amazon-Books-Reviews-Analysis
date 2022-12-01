from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import avg, when, collect_list
#import Matplotlib as plt
import sys
import re

conf = SparkConf().setMaster('local[*]').setAppName('librosPorAutoresSegunPrecio')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

'''
	Libros de autores buscados segun el rango de precio
		1er argumento Rango Bajo PRECIO
		2do argumento Rango Alto PRECIO
		3er en adelante argumentos NOMBRE AUTOR

'''

#Lectura  Argumentos
#author = "['" + sys.argv[1]
#author = author + "']"


#numero de argumentos que se le pasan al programa
num_args = len(sys.argv) # para lista de categorias bucle con esto para coger todas y al comprobar en el filter con todas ellas.
cat = []
for y in range(1,num_args):
    cat.append(sys.argv[y])

#author = "['Fiona Cownie']"
precio1 = cat[0]
precio2 = cat[1]

#EL nombre del autor esta entre [''] en el dataframe asique se lo añado a la busqueda del usuario
author ="['" + cat[2]
for x in range(3,len(cat)):
	author = author + " " + cat[x]
author = author + "']"

#Lectura archivo de los libros
df1 = spark.read.options(header='true').csv("books_data.csv")
df2 = spark.read.options(header='true').csv("reviewsSmall.csv")

#Renombro Columna titulo porque se llaman en los dos dataframes igual
df1 = df1.withColumnRenamed("Title", "Titulo")

#Uno dataframes por titulo
df = df1.join(df2, df2.Title == df1.Titulo, 'inner')

#Selecciono las columnas
df = df.select('authors',"Title", 'price').where(df["authors"] == author)


#Filtro los resultados segú el rango de precios seleccionado
df = df.filter(df.price.between(precio1, precio2))
#Agrupo por autor y titulo y pongo la media de los valoresd el libro(el mismo libro en dos momentos puede vales diferente)
df = df.groupby("authors","Title").agg(avg("Price"))

df.show()

