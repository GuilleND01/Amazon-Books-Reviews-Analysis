# Amazon-Books-Reviews-Analysis

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
- [mostRatings.py](/scripts/mostRatings.py): se obtiene un gráfico de barras con los N libros con más valoraciones en Amazon, su número y su media de rating. Esta información se guarda también en un CSV. 
- [formatCategory.py](/scripts/formatCategory.py): se obtiene un gráfico circular con los formatos de lectura más frecuentes para una categoría dada y sus porcentajes. En este se observarán los cinco formatos más leídos y el resto quedarán agrupados sobre la categoria allOtherCategories. Además, se generará un CSV con la cuenta de veces que un libro de la categoría se ha leido en ese formato. Se podrán ver todos aquellos formatos que no aparecían en el gráfico.
- [ratingsEvolution.py](/scripts/ratingsEvolution.py):
- [outstandingAuthors.py](/scripts/outstandingAuthors.py):
- [bestBooksCat.py](/scripts/bestBooksCat.py):
- [recommendedOrderedByRatings.py]((scripts/recommendedOrderedByRatings.py)



### Herramientas y entorno de trabajo
## 4. Resultados
## 5. Conclusiones

 
