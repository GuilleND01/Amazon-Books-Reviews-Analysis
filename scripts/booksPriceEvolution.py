from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import avg, when, collect_list
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as func
import matplotlib.pyplot as plt
import sys
import re

conf = SparkConf().setMaster('local[*]').setAppName('bookPerAuthorAndPrice')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

'''
	Precio de todos los libros a lo largo de un tiempo 
'''

##IMPORTANTE: 
#Ejemplo de entrada:	spark-submit .\booksPriceEvolution.py 2018 2019

#numero de argumentos que se le pasan al programa
num_args = len(sys.argv) # para lista de categorias bucle con esto para coger todas y al comprobar en el filter con todas ellas.
cat = []
for y in range(1,num_args):
    cat.append(sys.argv[y])

#Fecha en formato dia-mes-año
fecha1 = cat[0]
fecha2 = cat[1]

fecha1 = int(fecha1)
fecha2 = int(fecha2)

#Leo los datos de los libros
input_file1 = "../dataset/reviews.json" #valoraciones
input_file2 = "../dataset/metabooks.json" #libros

df = spark.read.json(input_file1)
df2 = spark.read.json(input_file2)

#Junto los dos dataframes por titulo
df = df.join(df2, df.asin == df2.asin, how = 'inner')

#Selecciono los libros, la fecha de la review de ese libro y el precio de ese libro
df = df.select(df["title"], df["reviewTime"], df["price"])

#Cogo los años dentro de la reviewTime porque tiene el formato "dd mm, yyyy"
df = df.withColumn('year', col('reviewTime').substr(6, 10))

#Le quito el $ a los precios, y la coma y los espacios a los años
df = df.select(df["title"], func.translate(func.col("price"), "$", "").alias("Precio"), func.translate(func.col("year"), ",", "").alias("Year"))
df = df.withColumn("Year", func.translate(func.col("Year"), " ", ""))
df = df.withColumn("Year", df["Year"].cast(IntegerType()))

#Asumo que el año en el que sale el libro es el año que mas reviews tiene
#calculo la moda de la fecha de la review por año y la media de sus precios
df = df.groupby("title").agg(func.avg("Year"), func.avg("Precio"))
df = df.withColumnRenamed("avg(Year)","Year")
df = df.withColumnRenamed("avg(Precio)","Precio")

df = df.withColumn('Year', col('Year').substr(0, 4).cast(IntegerType()))



#quitamos las columnas null
df = df.filter(df["Precio"].isNotNull())
df = df.withColumn('Precio', col('Precio').substr(0, 4))

#agrupo por año los precios 
df = df.groupby("Year").agg(func.avg("Precio"))

#Cambio un poco el nombre de las columnas y el numero de decimales
df = df.withColumnRenamed("avg(Precio)","Precio")
df = df.withColumn('Precio', col('Precio').substr(0, 4))

#Filto por los años que me interesan
df = df.filter(df["Year"].between(fecha1, fecha2))

df = df.withColumn('Precio', col('Precio').cast('float'))
df = df.withColumn('Year', col('Year').cast('string'))
#Ordeno por año
df = df.orderBy("Year", ascending = True)

df.show()

#df = df.filter(df["Year"]).between(fecha1, fecha2)

#df.show()

fig, ax = plt.subplots()

# Paso a array los títulos y sus reviews
yea = df.select(col('Year')).rdd.flatMap(lambda x: x).collect()
rat = df.select(col('Precio')).rdd.flatMap(lambda x: x).collect()

# Se pintan y unen los puntos
barGraphic = ax.scatter(yea, rat)
plt.plot(yea, rat)

ax.set_ylabel('Overall Price')
ax.set_title('Price evolution ' + cat[0] + ' ' + cat[1])

plt.xticks(rotation=90)
plt.savefig('priceEvolution' + cat[0] + cat[1] + '.png')

df.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/booksPriceEvolution.csv")
