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
Su cometido es, básicamente, generar la codificación. Para ello, hay una primera
clase y main (Pipeline y main_pipeline) que se encarga de hallar la cantidad
de usuarios únicos interseccionados y sus nombre; y una segunda clase y main
(codificacion y main_codificacion) se encarga de generar el dataframe de 
codificacion que se usará para alimentar la red neuronal utilidazon su pivote.

NN
Comprende el generador de datos, la nn y las baselines, un (futuro) filtrado de 
elementos en una clase separada y las métricas de evaluación. El generador de 
datos empaqueta los datos en tf.data.Dataset y es la más compleja. Primero criba
los elementos de las imágenes por aquellas que tengan al menos una review y una 
imagen válida. Tras esto, cada elmeneto de la ciudad a se repite tantas veces
como reviews tenga, y a cada registro repetido se le asigna una imagen aleatoria
de ese elemento. Luego, se mapea una función en tf.data para que cargue esa 
imagen en la nn. Todo esto de forma muy configurable. El df de coficiación
también se filtra a nivel de elementos de la ciudad b (normalmente pois) 
en función de las etiquetas extraídas en TripAdvisor (por defecto, elmina todas
aquellas que tengan "tour" o "taxi"), los ordena de mayor a menor con respecto
al número de reviews, y escoge tantos como se le indiquen. Por otro lado, 
nn_1 almacena una de las nn que hice para probar el sistema. Se probaron más 
versiones pero sin resultados significativos (más adelante lo explico). 
Originalmente, se tenía la idea de hacer una interfaz de la que heredasen cada
piloto de nn, pero no dio tiempo al desarrollo. Igualmente, es muy suberible 
hacerlo para automatizar las evaluaciones en el futuro. Cuando se entrena la nn,
se crea una carpeta en la que los callbacks guardan el mejor modelo y, al final,
también una imágen de la loss de entrenamiento vs la de evaluación. Todo esto
se hace con el main_entrenamiento_nn. Además, Métricas almacena las métricas de
evaluación de los modelos y baselines la creación de las baselines (zeros y pop).
La evaluación de los rankings se hace por la Tau de Kendall (distancia, que 
está acotada entre 0 y 1, a diferencia de la corr, que está acotada entre -1 y 1).
La evaluación se hace en main_evaluar_nn.

Conclusiones de lo relizado
He podido trabajar poco con la nn debido a la demora que ha sido diseñar 
correctamente la extracción, pero con las sucesivas pruebas que he hecho he sido
incapaz de conseguir que los modelos aprendan con un número moderado o grande de
pois. En mi opinión, existen demasiados 0s a medida que se incrementa el número
de pois considerados en la codificación y el modelo apenas puede distinguierlos.
Las curvas de aprendizaje (loss) mustran que solo aprende en la primera epoch, 
a partir de ahí el error es muy bajo y se estanca. También hay muchos valores
muy parecidos cercanos a 0 y cuya diferencia, para la nn, se antoja aleatoria 
incluso intentando añadir capas de batch normalization para evitar la evanescencia
de la información. Creo que la continuación del proyecto debe considerar el uso
clasificación y otra información procedente de TripAdvisor. Por otro lado, 
depender de la cantidad absoluta de visitantes de elementos de la ciudad a con 
elementos de la ciudad b para calcular los rankings me parece que es un callejón
sin salda aun cuando se consiga demostrar su eficacia, puesto que es parte de la
información desconocida para la red. Creo que se debería reducir la cantidad
de pois a través de agrupaciones en función de su semejanza. 

Otros comentarios
Entiendo que ha sido un proyecto frustrante al tener tantos problemas durante la 
extracción, y es que el código a terminado por ser muy grande y, aunque he intentado
que fuera fácil de mantener, entiendo que puede resultar complicado. Por ello, 
si alguna vez se tiene algina duda, mandadme sin problema un correo con las 
dudas y os las soluciono sin problema