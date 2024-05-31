from src.nn.constructor_de_rutas import ConstructorDeRutas
from src.nn.filtro_generador import FiltroGenerador
from src.nn.generador2 import Generador
from src.nn.nn_1 import NN
from src.nn.baselines import Baselines
from src.nn.metricas import Metricas

import tensorflow as tf
import pandas as pd
import numpy as np
from scipy.stats import spearmanr
import pdb

BATCH_SIZE = 264
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
    df_codificacion_normalizada_nn = generador.df_codificacion_normalizada.loc[elementos_x, elementos_y]
    
    dataset_y = df_codificacion_normalizada_nn.values
    dataset_y = dataset_y.astype(np.float32)

    dataset_y = tf.data.Dataset.from_tensor_slices(dataset_y)
    dataset_x = tf.data.Dataset.from_tensor_slices(dataset_x)

    dataset = generador.crear_dataset( dataset_y, dataset_x, batch_size=BATCH_SIZE )

    return dataset, df_codificacion_normalizada_nn


def agrupar_y_obtener_metricas_prediccion_nn(df_pred_nn:pd.DataFrame):
    df_media = df_pred_nn.groupby('elementos a').mean()
    # df_std = df_pred_nn.groupby('elementos a').std()
    return df_media


if __name__ == '__main__':
    # Generador de datos
    rutas = ConstructorDeRutas('Madrid', 'Rome_Lazio')
    generador = Generador( rutas, (0.7, 0.3, 0), eje_normalizacion_intersecciones=1 )

    # elementos_x_train = generador.elementos_dataset_train
    # dataset_train, df_codificacion_nn_train = crear_dataset(elementos_x_train, generador)

    elementos_x_val = generador.elementos_dataset_val
    dataset_val, df_codificacion_normalizada_nn = crear_dataset(elementos_x_val, generador)
    df_codificacion_nn = generador.df_codificacion.loc[generador.elementos_dataset_val]
    # pdb.set_trace()

    baselines = Baselines(df_codificacion_nn, rutas)
    rmse_zeros, mse_zeros = baselines.obtener_rendimiento_baseline_zeros()
    rmse_pop, mse_pop = baselines.obtener_rendimiento_baseline_popularidad()

    modelo = NN.cargar_modelo_entrenado(rutas)
    y_nn_val_pred = modelo.predict(dataset_val, verbose=0)
    mse_modelo_nn = Metricas.mse(df_codificacion_normalizada_nn.values, y_nn_val_pred)
    rmse_modelo_nn = Metricas.rmse(df_codificacion_normalizada_nn.values, y_nn_val_pred)

    # Rankings
    # Se convierte el tensor predicho a un df
    index = df_codificacion_normalizada_nn.index
    cols = df_codificacion_normalizada_nn.columns
    df_y_pred_nn = pd.DataFrame(y_nn_val_pred, columns=cols)
    df_y_pred_nn['elementos a'] = index
    df_y_pred_agrupado_nn = agrupar_y_obtener_metricas_prediccion_nn(df_y_pred_nn)
    metricas_rankings = Metricas.obtener_metricas_rankings(df_y_pred_agrupado_nn)
    print(metricas_rankings)

    valores_resultados = {
        'rmse': [rmse_zeros, rmse_pop, rmse_modelo_nn], 
        'mse': [mse_zeros, mse_pop, mse_modelo_nn]}
    df_resultados_baselines = pd.DataFrame(
        valores_resultados, index=['Zeros', 'Pop', 'Modelo'])

    print('Resultados Validacion:')
    print(df_resultados_baselines)