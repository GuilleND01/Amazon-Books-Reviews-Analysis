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
#Ejemplo de entrada:	spark-submit .\booksPriceEvolution.py 2010 2018 Medicine

#Fecha en formato dia-mes-año
fecha1 = sys.argv[1]
fecha2 = sys.argv[2]

cat = sys.argv[3]
cat1 = cat.replace('&', '&amp;')


fecha1 = int(fecha1)
fecha2 = int(fecha2)

#Leo los datos de los libros
input_file1 = "../dataset/reviews.json" #valoraciones
input_file2 = "../dataset/metabooks.json" #libros

df = spark.read.json(input_file1)
df2 = spark.read.json(input_file2)

df = df.join(df2, df.asin == df2.asin, how = 'inner')

#Selecciono los libros, la fecha de la review de ese libro y el precio de ese libro
df = df.select(df["title"], df["reviewTime"], df["price"], df["category"])

# Se filtran según la categoría introducida
df = df.filter(func.array_contains(df['category'], cat) | 
    func.array_contains(df['category'], cat1))

if df.isEmpty():
    sys.exit()

#Cogo los años dentro de la reviewTime porque tiene el formato "dd mm, yyyy"
df = df.withColumn('year', col('reviewTime').substr(6, 10))

#Le quito el $ a los precios, y la coma y los espacios a los años
df = df.select(df["category"], func.translate(func.col("price"), "$", "").alias("Precio"), func.translate(func.col("year"), ",", "").alias("Year"))
df = df.withColumn("Year", func.translate(func.col("Year"), " ", ""))
df = df.withColumn("Year", df["Year"].cast(IntegerType()))
df = df.withColumn('Year', col('Year').substr(0, 4).cast(IntegerType()))

#Asumo que el año en el que sale el libro es el año que mas reviews tiene
#calculo la moda de la fecha de la review por año y la media de sus precios
df = df.groupby("Year").agg(func.avg("Precio"))
df = df.withColumnRenamed("avg(Precio)","Precio")


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

df.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("../results/categoryPriceEvolution.csv")

fig, ax = plt.subplots()

# Paso a array los títulos y sus reviews
years = df.select(col('Year')).rdd.flatMap(lambda x: x).collect()
prices = df.select(col('Precio')).rdd.flatMap(lambda x: x).collect()

# Se pintan y unen los puntos
barGraphic = ax.scatter(years, prices)
plt.plot(years, prices)

ax.set_ylabel('Price')
ax.set_title('Evolution of the category ' + cat)

plt.xticks(rotation=90)
plt.savefig('../results/' + 'PriceEvolutionOf' + cat + '.png')
