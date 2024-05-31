El programa consta de 3 módulos diferenciados: Scraper (se encarga de exrtaer la
información de tripadvisor), Pipeline (transforma de forma básdica los datos
para agilizar el procesado) y Nn (correspondiente a la parte de generación de 
datos, red neuronal, baselines, entrenamiento y evaluación). Los únicos archivos
que pudieran ser editables en un principio serían los main, que son los únicos 
ejecutables para obtener información.

Scraper
Ataca directamente a la sql graph intermedia que almacena los datos de 
tripadvisor. Consta de dos clases gemelas, PoisTripAdvisor y 
RestaurantesTripAdvisor. Cada una de ellas extrae datos de pois y restaurantes
de una ciudad, respectivamente. Cada una de ellas utiliza una clase extractora
para cada poi/restaurante, que es la que se encarga de sacar los datos y 
formateerlos de cada establecimiento. Funcionan con paralelización debido a que
los procesos tienen la mayoría del tiempo operaciones i/o. Es importante que, 
si no se quieren reemplazar los datos ya scrapeados de una ciudad, no se lance
la función "descargar_informacion_de_restaurante/poi", porque genera una 
reescritura. Por ello, el main tiene un ejemplo activo del scraping de Londres
en el que se mira si se han extraído datos anteriormente para continuarlos. 
Tened en cuenta que, por numerosas razones, el programa puede no extraer 
adecuadamente los datos de algún elemento (que, dependiendo del caso, a veces no
es fácil de ver en los loggings debido al asincronismo de las consultas). 
Ya sea por fallos de conexión, bloqueos, malas respuestas de la sql, etc. Por 
ello, es sano que, terminado el scraping, se relance un par de veces por si 
quedasen residuos. Además, hay ciertas reviews que pueden quedar eliminadas en 
la sql, por lo que no siempre se devolverán todos los elementos de una página. 
El programa realiza varias consultas para asegurarse antes de pasar a la siguiente
página del elemento. Los datos scrapeados se almacenan en su homónima carpeta de 
data. 

Pipeline
