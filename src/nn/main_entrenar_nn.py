from src.nn.constructor_de_rutas import ConstructorDeRutas
from src.nn.filtro_generador import FiltroGenerador
from src.nn.generador2 import Generador
from src.nn.nn_1 import NN

import tensorflow as tf
import pandas as pd
import numpy as np

BATCH_SIZE = 512
TAMANIO_VECTOR_CODIFICACION = 1000


def crear_dataset(elementos_x:np.ndarray, generador:Generador) -> tf.data.Dataset:
    """
        elementos_x es el conjunto de nombres de los elementos de la ciudad a,
        normalmente restaurantes. Pueden ser los de entranamiento, validación o
        test 
    """
    elementos_x, dataset_x = generador.expandir_elementos_a_con_usuarios_reviews_y_asignar_imagenes(
        elementos_x)

    elementos_y = FiltroGenerador.obtener_pois_seleccionados_por_popularidad_y_etiquetas(
        generador.parametros_conjunto.ciudad_b, TAMANIO_VECTOR_CODIFICACION)
    dataset_y = generador.df_codificacion_normalizada.loc[elementos_x, elementos_y].values
    dataset_y = dataset_y.astype(np.float32)

    dataset_y = tf.data.Dataset.from_tensor_slices(dataset_y)
    dataset_x = tf.data.Dataset.from_tensor_slices(dataset_x)

    dataset = generador.crear_dataset( dataset_y, dataset_x, batch_size=BATCH_SIZE )

    return dataset


if __name__ == '__main__':
    # Generador de datos
    rutas = ConstructorDeRutas('Madrid', 'Rome_Lazio')
    generador = Generador( rutas, (0.7, 0.3, 0), eje_normalizacion_intersecciones=1 )

    elementos_x_train = generador.elementos_dataset_train
    dataset_train = crear_dataset(elementos_x_train, generador)

    elementos_x_val = generador.elementos_dataset_val
    dataset_val = crear_dataset(elementos_x_val, generador)
    
    datasets = (dataset_train, dataset_val, None)

    nn = NN(rutas, datasets, TAMANIO_VECTOR_CODIFICACION)
    nn.fit()