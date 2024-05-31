import pandas as pd
import numpy as np
import random
import os
import math
# os.environ["CUDA_VISIBLE_DEVICES"] = "1"

import tensorflow as tf
from matplotlib import pyplot as plt
import seaborn as sns
import cv2
from src.nn.constructor_de_rutas import ConstructorDeRutas
from src.nn.filtro_generador import FiltroGenerador
from typing import Tuple, Literal

SEMILLA = 54


def cargar_y_procesar_imagen(ruta_imagen:np.ndarray, escalado:tuple = (299, 299)) -> np.ndarray:
    tf.print("Original path:", ruta_imagen)
    # pdb.set_trace()
    ruta_imagen = ruta_imagen.decode('utf-8')  # Decodificación de los bytes
    tf.print("Decoded path:", ruta_imagen)
    
    imagen = tf.keras.preprocessing.image.load_img(ruta_imagen, target_size=escalado)
    imagen_array = tf.keras.preprocessing.image.img_to_array(imagen)
    imagen_array = (imagen_array / 127.5) - 1.0  # Normalizar [-1, 1]
    
    imagen_array = imagen_array.astype(np.float32)  # Nos aseguramos de que sean del tipo correcto
    tf.print("Processed image shape:", imagen_array.shape, "dtype:", imagen_array.dtype)
    
    return imagen_array


class Generador:
    def __init__(self, rutas:ConstructorDeRutas, splitter:tuple, eje_normalizacion_intersecciones:Literal[-1, 0, 1]) -> None:
        self.parametros_conjunto = rutas
        self.__establecer_df_codificacion_y_filtrar_elementos_a_validos()
        if sum(splitter) != 1:
            raise ValueError('El splitter debe ser un triplete de tres valores entre 0 y 1 que sumados den 1, y que representan la proporción de entrenamiento, validación y test')
        self.splitter = splitter      
        self.normalizar_codificacion(axis=eje_normalizacion_intersecciones)
        elementos_a_train, elementos_a_val, elementos_a_test = self.__barajar_y_dividir_elementos_a()
        self.__elementos_a_train = elementos_a_train
        self.__elementos_a_val = elementos_a_val
        self.__elementos_a_test = elementos_a_test

    @property
    def elementos_dataset_train(self):
        return self.__elementos_a_train


    @property
    def elementos_dataset_val(self):
        return self.__elementos_a_val


    @property
    def elementos_dataset_test(self):
        return self.__elementos_a_test


    def crear_dataset(self, dataset_y:tf.data.Dataset, dataset_x:tf.data.Dataset, batch_size: int) -> None:
        # Crea el dataset
        def _process_image(image_path):
            processed_image = tf.numpy_function(
                cargar_y_procesar_imagen,
                [image_path],
                tf.float32 # Si hubiese más tipos se podría con un array 
            )
            processed_image.set_shape((299, 299, 3))  # Todas deben de tener el mismo tamaño
            return processed_image

        dataset_x = dataset_x.map(
            _process_image,
            num_parallel_calls=tf.data.experimental.AUTOTUNE
        )

        tf.print('Mapping of dataset_x done')
        
        dataset = tf.data.Dataset.zip( (dataset_x, dataset_y) )
        dataset = dataset.batch( batch_size )
        dataset = dataset.prefetch( buffer_size=tf.data.experimental.AUTOTUNE )
        
        return dataset
    

    def obtener_pois_seleccionados_por_popularidad_y_etiquetas(
            ciudad_pois:str, n_pois:int, etiquetas:list=None) -> np.ndarray:
        # Devuelve una lista con los pois seleccionados en base al número de pois 
        # (n_pois) con más reviews y las etiquetas (por defecto, todas aquellas  
        # que no contengan "tour")
        ruta_listado_pois = './data/scraping/pois/' + ciudad_pois + '/listado_pois.parquet'
        df_listado_pois = pd.read_parquet(ruta_listado_pois)
        df_listado_pois_ordenado = df_listado_pois.sort_values(by='cantidad reviews', ascending=False)
        
        if etiquetas is None:
            funcion_lambda_seleccion = lambda etiquetas_fila: \
                'tours' not in etiquetas_fila.lower() and 'cooking classes' not in etiquetas_fila.lower()
        else:
            funcion_lambda_seleccion = lambda etiquetas_fila: \
                np.isin(etiquetas_fila.split(';'), etiquetas).sum() > 0

        # Puede haber duplicados, con lo que se eliminan
        df_listado_pois_ordenado.drop_duplicates(subset=['Id'], inplace=True)

        pois_seleccionados = df_listado_pois_ordenado.loc[
            df_listado_pois_ordenado['tags'].map(funcion_lambda_seleccion), 
            'nombre'].iloc[:n_pois].values
        
        return pois_seleccionados


    def normalizar_codificacion(self, axis:int=1) -> None: 
        # Normalmente las filas son restaurante y las columnas por pois. Por tanto, 
        # si se hace la normalización por el eje 1, se hace a nivel de totales 
        # de restaurante, si no sería en base a los totales de los pois
        # La estructura del df es: index filas: 
        # nombre restaurante, index cols: nombre pois, valores: recuento intersecciones usuarios unicos
        ruta_cod = self.parametros_conjunto.obtener_ruta_pipeline() + 'df_codificacion.parquet'
        df_codificacion_sin_filtrar = pd.read_parquet(ruta_cod) 

        if axis == 0: # Normalización por columnas
            # Vector fila para hacer el cast
            vector_normalizacion = df_codificacion_sin_filtrar.values.sum(axis=0)
        elif axis == 1: # Normalización por filas
            # Vector columna para hacer el cast
            vector_normalizacion = df_codificacion_sin_filtrar.values.sum(axis=1).reshape(-1, 1) 
        else:
            raise ValueError('axis debe ser 1 o 0')
        vector_normalizacion[vector_normalizacion == 0] = 1 # Para evitar errores de div / 0
        df_codificacion_normalizada = df_codificacion_sin_filtrar / vector_normalizacion

        # Se cogen los restaurantes válidos
        df_codificacion_normalizada = df_codificacion_normalizada.loc[self.df_codificacion.index]

        self.df_codificacion_normalizada = df_codificacion_normalizada


    def __barajar_y_dividir_elementos_a(self) -> Tuple[np.ndarray]:
        # Devuelve los tres conjuntos de datos habiendo barajado
        elementos_a = pd.DataFrame(self.df_codificacion.index)
        # Se baraja
        elementos_a = elementos_a['restaurantes'].sample(frac=1, random_state=SEMILLA).to_numpy()

        # Se divide
        frac_train, frac_val, frac_test = self.splitter
        tamanio_poblacion = len(elementos_a)
        lim_sup_train = math.trunc(tamanio_poblacion * frac_train)
        lim_sup_val = lim_sup_train + math.trunc(tamanio_poblacion * frac_val)

        elementos_a_train = elementos_a[:lim_sup_train]
        elementos_a_val = elementos_a[lim_sup_train:lim_sup_val]
        elementos_a_test = elementos_a[lim_sup_val:]

        return elementos_a_train, elementos_a_val, elementos_a_test
    

    def expandir_elementos_a_con_usuarios_reviews_y_asignar_imagenes(
            self, elementos_a_sin_repetir:np.ndarray, orden_aleatorio:bool=True) -> Tuple[np.ndarray]:
        """ Función que, dado un conjunto de elementos (normalmente los elementos
            de la ciudad a en su forma dividida de entrenamiento, validación o test),
            repite cada uno de ellos tantas veces como usuarios hayan dejado reviews
            y asigna a cada uno de ellos una imagen aleatoria del repositorio de 
            imagenes de ese elemento, devolviendo un vector de elementos repetidos
            para poder generar sus matriz de codificación y otro mapeadao con este
            de rutas de imagenes asignadas
        """
        ruta_list_restaurantes = self.parametros_conjunto.obtener_ruta_scraping(de_ciudad_a=True) + 'listado_restaurantes.parquet'
        df_listado_a = pd.read_parquet(ruta_list_restaurantes, columns=['Id', 'nombre'])
        df_listado_a = df_listado_a.loc[np.isin(df_listado_a['nombre'], elementos_a_sin_repetir)]
        archivos_reviews = df_listado_a.apply(lambda fila: str(fila['Id']) + '_' + fila['nombre'] + '.parquet', axis=1).values

        lista_elementos_a_ampliados = []
        lista_imagenes_escogidas = []
        cont_elementos_correctos = 0
        cont_elementos_incorrectos = 0
        print('Archivos erróneos durante la extensión:')
        for f_nombre, elemento in zip(archivos_reviews, elementos_a_sin_repetir):
            f = self.parametros_conjunto.obtener_ruta_scraping(de_ciudad_a=True) + 'reviews/' + f_nombre
            if os.path.exists(f):
                try:
                    # Si el archivo no existe o no cuenta con columnas (es decir, 
                    # no tiene reviews) dará error. Se ignorará.
                    elemento_repetido = np.repeat( elemento, len(pd.read_parquet(f, columns=['usuario'])) )
                    # Si no tiene imágenes, no tentrá directorio y dará error. Se ignorará.
                    lista_imagenes_escogidas.append( self.__asignar_imagen_aleatoria_a_elemento(elemento, len(elemento_repetido)) )
                    # Si se llega a este punto, sí que se puede incluir 
                    lista_elementos_a_ampliados.append(elemento_repetido)
                    cont_elementos_correctos += 1
                except Exception as e: # Continuar si no tiene columnas
                    print('\tImgs ' + f)
                    cont_elementos_incorrectos += 1

            else: # Hay restaurantes que tienen / en su nombre y son incorrectos por ser ruta de directorio
                print('\tReviews ' + f)
                cont_elementos_incorrectos += 1

        print('Archivos procesados')
        print(f'Cantidad de elementos con reviews e imágenes: {cont_elementos_correctos}')
        print(f'Cantidad de elementos sin reviews y/o imágenes: {cont_elementos_incorrectos}')
        concatenado_elementos_a = np.concatenate( lista_elementos_a_ampliados, axis=None )
        concatenado_imagenes_a = np.concatenate( lista_imagenes_escogidas, axis=None )
        
        if orden_aleatorio:
            np.random.seed(SEMILLA)
            np.random.shuffle(concatenado_elementos_a)
            np.random.seed(SEMILLA)
            np.random.shuffle(concatenado_imagenes_a)

        return concatenado_elementos_a, concatenado_imagenes_a
    

    def __asignar_imagen_aleatoria_a_elemento(self, elemento_a:str, cantidad_imagenes:int) -> np.ndarray:
        ruta_dir_imagen = self.parametros_conjunto.obtener_ruta_imagenes(de_ciudad_a=True) + elemento_a + '/'
        np.random.seed(SEMILLA)
        imagenes_seleccionadas = np.char.array(np.random.choice([f for f in os.listdir(ruta_dir_imagen) if f.endswith('.jpg')], cantidad_imagenes))
        rutas_imagenes_seleccionadas = np.char.array(np.repeat(ruta_dir_imagen, len(imagenes_seleccionadas))) + imagenes_seleccionadas
        return rutas_imagenes_seleccionadas


    def __establecer_df_codificacion_y_filtrar_elementos_a_validos(self) -> None:
        ruta_cod = self.parametros_conjunto.obtener_ruta_pipeline() + 'df_codificacion.parquet'
        df_codificacion = pd.read_parquet(ruta_cod)
        vector_filtro_a = []
        cont_elementos_correctos = 0
        cont_elementos_incorrectos = 0

        print('Descartando elementos de la ciudad a que carecen de reviews o imagenes...')

        for elemento_a in df_codificacion.index:
            ruta_dir_imagenes = self.parametros_conjunto.obtener_ruta_scraping(
                de_ciudad_a=True) + 'imagenes/' + elemento_a
            if os.path.exists(ruta_dir_imagenes):
                lista_imagenes = [f for f in os.listdir(ruta_dir_imagenes) if f.endswith('.jpg')]
                filtro_elemento = len(lista_imagenes) > 0
            else:
                filtro_elemento = False

            vector_filtro_a.append(filtro_elemento)
            if filtro_elemento:
                cont_elementos_correctos += 1
            else:
                cont_elementos_incorrectos += 1

        print(f'Cantidad de elementos con reviews e imágenes: {cont_elementos_correctos}')
        print(f'Cantidad de elementos sin reviews y/o imágenes: {cont_elementos_incorrectos}')

        self.df_codificacion = df_codificacion.loc[vector_filtro_a]

