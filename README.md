# Amazon-Books-Reviews-Analysis

[Web](https://booksreviews.cloudaccess.host/)
## 1. Introducción
Cada día en Amazon se venden miles de productos, entre los que por suspuesto están los **libros**. Las ventas de libros en Amazon supusieron un 15% de los ingresos totales durante 2021. O lo que es lo mismo, un 15% de los 469.822 millones de dólares que facturó la plataforma el pasado año. Además, durante los últimos años más editoriales y autores han optado por las facilidades que esta plataforma ofrece.

Nuestro estudio propone un análisis de los datos generados por las **valoraciones** de los usuarios y los libros que se pueden comprar. Esta información será útil para las próximas compras o decisiones del consumidor, además de para el escritor o marcas que publiquen en Amazon, pues se podrán obtener conclusiones sobre comportamientos futuros del cliente.

La necesidad de técnicas de **Big Data** y **procesamiento paralelo** en nuestro estudio recae en la gran cantidad de datos a tratar, en la búsqueda de un mayor rendimiento y reducción del tiempo de ejecución y en la necesidad de dividir los datos en conjuntos más pequeños para procesar más en menos tiempo. Para así lograr una mayor precisión en los resultados y fiabilidad de los mismos, gracias al uso de métodos más complejos.

## 2. Modelo de datos y origen
Nuestros datos han sido obtenidos de dos datasets. El primero de ellos contiene información sobre las **reviews de los usuarios**, incluyendo la puntuación que se ha dado al libro, el nombre de usuario de la persona, su identificador, el identificador del producto, la review completa y un resumen, el id del producto que se ha valorado, etc. El segundo contiene **metadatos** de cada uno de los **libros**: título, precio, categorías a las que pertenece dentro de Amazon, índice de ventas, descripción, productos comprados parecidos a este, etc.

Los ficheros son los siguientes:
- **reviews.json**, que comprende 27,164,983 reviews entre 1996 y 2018 (**21GB**)
- **metabooks.json**, con información de 2,935,525 libros (**4GB**)

En la carpeta [datasets](/dataset) de este repositorio se encuentra una versión reducida de ambos archivos debido a las limitaciones de la plataforma. Los datos han sido obtenidos de la web de Amazon por Jianmo Ni y se pueden visitar y descargar desde [aquí](https://nijianmo.github.io/amazon/index.html). 

## 3. Descripción técnica
### Software
Se han desarrollado los siguientes scripts en Python y se pasa a hacer una breve descripción de ellos. Pulsando en el nombre de este se puede ver el código, alojado en la carpeta [scripts](/scripts).
- [**mostRatings.py**](/scripts/mostRatings.py): se obtiene un gráfico de barras con los N libros con más valoraciones, su número y su media de rating. Esta información se guarda también en un CSV. Ej: _spark-submit mostRatings.py N_
- [**formatCategory.py**](/scripts/formatCategory.py): se obtiene un gráfico circular con los formatos de lectura más frecuentes para una categoría dada y sus porcentajes. En este se observarán los cinco formatos más leídos y el resto quedarán agrupados sobre la categoria allOtherCategories. Además, se generará un CSV con la cuenta de veces que un libro de la categoría se ha leido en ese formato. Se podrán ver todos aquellos formatos que no aparecían en el gráfico. Ej: _spark-submit formatCategory.py category_
- [**booksPerAuthorAndPrice.py**](/scripts/booksPerAuthorAndPrice.py): el usuario introduce dos precios y el nombre de un autor y el programa devuelve los libros del autor introducido que tengan un precio comprendido entre el primer numero y el segundo. 
Ej: _spark-submit booksPerAuthorAndPrice 10 40 John Ruskin_
- [**booksPriceEvolution.py**](/scripts/booksPriceEvolution.py):
- [**ratingsEvolution.py**](/scripts/ratingsEvolution.py): mediante la introducción de una categoría concreta, el programa muestra un gráfico con la puntuación media que han recibido los libros de ese género en cada uno de los años. Ej: _spark-submit ratingsEvolution.py 'Education & Reference'_
- [**outstandingAuthors.py**](/scripts/outstandingAuthors.py): muestra los autores que tienen una mejor puntuación media en sus obras y a igualdad de puntuaciones se tiene en cuenta el número de valoraciones realizadas. Ej: _spark-submit outstandingAuthors.py Paranormal_
- [**bestBooksCat.py**](/scripts/bestBooksCat.py): mediante la intoducción de una categoria o un conjunto de categorías, el programa obtiene los libros que pertenecen a ese conjunto de categorías mostrados de mejor a menor para facilitar al cliente la elección de un libro y acertar. Estos libros se muestran de mejor a peor valoración y a su vez se valorara mejor un libro con mas reviews que otro en caso de tener la misma valoración. Ej: _spark-submit bestBooksCat.py "Medical Books" "Medicine"
- [**mostPopularCategories.py**](/scripts/mostPopularCategories.py): El programa muestra cuales son las categorías mas populares, mediante el número de valoraciones realizadas en estos libros para facilitar así a los escritores poder elegir una categoria para su próximo libro que pueda triunfar entre los lectores. Ej: _spark-submit mostPopularCategories.py
- [**recommendedOrderedByRatings.py**](/scripts/recommendedOrderedByRatings.py): muestra una tabla de recomendaciones de libros dado el título de un libro, basado en las compras de usuarios que han comprado el título proporcionado. La tabla está ordenada por las valoraciones medias y el número de valoraciones de los libros recomendados. Esta información se guarda en un CSV. Ej: _spark-submit recommendedOrderedByRatings.py Matisse
- [**recommendedByViewed.py**](/scripts/recommendedByViewed.py): muestra una tabla de recomendaciones de libros dado el título de un libro, basado en las búsquedas de libros de usuarios que han buscado el título proporcionado. La tabla está ordenada por las valoraciones medias y el número de valoraciones de los libros recomendados. Esta información se guarda en un CSV. Ej: _spark-submit recommendedByViewed.py Matisse

Destacar que en algunos de estos scripts ha sido necesario contemplar algunos errores que los datasets contenían. En metabooks.json algunas categorías aparecían de la siguiente manera: ["Books", "Education &amp: Reference"], cuando debería ser Education & Reference. Tambíen, el nombre del autor (columna _brand_) en ocaciones contiene: "Visit Amazon's Sylvie Raulet Page", cuando debería ser Sylvie Raulet. Estos errores se han gestionado en el script, pues se ha decidido no hacer un preprocesado y filtrado del dataset.  

### Herramientas y entorno de trabajo
Para el desarrollo del estudio nos hemos ayudado de las siguientes herramientas y tecnologías:
1. **Google Cloud**, para la ejecución de los scripts, almacenamiento y gestión de datos.
2. **GitHub**, para la gestíon de ficheros y su control de versiones. 
3. **Python**, como lenguaje de programación de los scrips presentados como solución.
4. **Matplotlib**, como librería de Python de creacción de gráficos de dos dimensiones para presentar los resultados.
5. **WordCloud**, como librería de Python de creacción de nubes de palabras (representación visual de las palabras que conforman un texto, en donde el tamaño es mayor para las palabras que aparecen con más frecuencia)
6. **PySpark**, como interfaz de Apache Spark en Python, para realizar operaciones sobre los datasets y hacer uso de una programación funcional paralela.
7. **Excel**, para la representación gráfica de los resultados obtenidos en el análisis de rendimiento.
8. **WordPress**, para la página web.

### Probar nuestro estudio
Se pasa a detallar los pasos para la reproducción de nuestro estudio en una instancia de Google Cloud o en un equipo Ubuntu.
1. **Instalación de Java**<br />
 ```
 $ sudo apt install default-jre
 $ java -version
 ```
2. **Instalación de Python**
```
$ sudo apt-get update
$ sudo apt-get install python3.6
```
3. **Instalación de Spark**
```
$ curl -O https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
$ tar xvf spark-3.3.1-bin-hadoop3.tgz
$ sudo mv spark-3.3.1-bin-hadoop3 /usr/local/spark
```
4. **Configuración del entorno** <br />
Se añade /usr/local/spark/bin al PATH en el fichero ~/.profile. Después de actualiza el PATH en la sesión actual.
```
$ echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
$ source ~/.profile
```
5. **Instalación de librerias**
```
$ sudo apt install python3-pip
$ pip install wordcloud
$ sudo apt-get install python3-matplotlib
```
6. **Descarga de archivos** <br />
Se podrán crear las carpetas [scripts](/scripts), [dataset](/dataset) y [results](/results) de manera manual y copiar los datasets y los scripts en las correspondientes, aunque también es posible descargar este repositorio y descomprimirlo, desplegando el botón _<> Code_ y marcando _Download ZIP_. Tener en cuenta que la carpeta results tendrá algunos resultados ya.
7. **Ejecución de scripts** <br />
Colocados en la carpeta /scripts damos la orden a Spark de ejecución del programa. Los argumentos pasados dependerán del script y se han detallado más arriba.
```
$ spark-submit mostRatings.py 4
```
Los resultados aparecerán en la carpeta /results 
<br />
## 4. Resultados
Se pueden encontrar algunos resultados del estudio en la carpeta [results](/results) de este repositorio, aunque en la [web](https://booksreviews.cloudaccess.host/) se explican estos con más detalle.
### Rendimiento
Ejecutamos el programa bestBoksCat.py desde distintas maquinas y distintos hilos, workers y ejecutores para poder comprobar la diferencia de tiempo para su ejecución.
Para ello empleamos las siguientes pruebas:

En Maquina Local:

Ejecutamos en la maquina Local el programa con 4,8,12,16 hilos y cada uno con distintas particiones (100,200,300) viendo así la diferencia de tiempo entre los distintos casos probados y podiendo hacernos una idea de en que ocasiones es mas óptimo.

![Local (1)](https://user-images.githubusercontent.com/91116613/206902103-3d0539bd-adcc-40ea-b7a0-8da472606419.png)

En Maquina n1-standard-4 ejecutando 4 hilos:

Ejecutamos el programa para 4 hilos siempre, cambiando el numero de ejecutores y de workers para ver como afectan al rendimiento del programa. Viendo en los gráficos obtenidos la necesidad de controlar el numero de ejecutores y workers para obtener un mejor resultado. 

![n1-standard-4 (4 hilos)](https://user-images.githubusercontent.com/91116613/206902115-87f7763d-fbdb-43c9-8990-b161f07d9c71.png)

En Maquina n1-standard-4 con 2 workers especializados en ejecución de 4 hilos:

Ejecutamos el programa con 4 hilos y en este caso empleamos dos workers especializados en 4 hilos para ver la diferencia con la anterior máquina. En esta comprobamos también la mejora al aumentar el numero de ejecutores del programa. la diferencia con la anterior maquina sin workers especializados es bastante notable.

![n1-standard-4 ( 2 c2-standard-4 workers   4 Hilos)](https://user-images.githubusercontent.com/91116613/206902111-6e444beb-67bf-4949-8249-d6693e198a66.png)

En Maquina n1-standard-8 con 2 workers ejecutando 8 hilos:

Ejecutamos el programa con 8 hilos y modificando el numero de ejecutores para comprobar el rendimiento al ejecutar nuetro programa donde podemos apreciar que hay una diferencia muy significativa entre tener pocos ejecutores y al aumentarlos. Llega un momento que la diferencia de rendimiento al aumentar el numero de ejecutores es bastante pequeña, pero aún asi sigue disminuyendo un poco.

![n1-standard-8 (2 Workers   8 Hilos)](https://user-images.githubusercontent.com/91116613/206902118-90be79f8-865e-419f-b826-8d04048de0af.png)

## 5. Conclusiones
Las conclusiones finales del estudio, objetivos alcanzados, aprendizaje y posibles extensiones futuras del trabajo se recogen también en la [web](https://booksreviews.cloudaccess.host/)
 
