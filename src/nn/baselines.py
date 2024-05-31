import tensorflow as tf
import pandas as pd
import numpy as np
import typing
import pdb

from src.nn.metricas import Metricas
from src.nn.constructor_de_rutas import ConstructorDeRutas


class Baselines:
    def __init__(
            self, df_codificacion_nn:pd.DataFrame, rutas:ConstructorDeRutas
            ) -> None:
        self.__rutas = rutas
        self.__df_codificacion_nn = df_codificacion_nn
        self.__elementos_x_seleccionados = self.__df_codificacion_nn.index
        self.__elementos_y_seleccionados = self.__df_codificacion_nn.columns

    
    def obtener_rendimiento_baseline_zeros(self) -> typing.Tuple[float]:
        valores_codificacion_nn = self.__df_codificacion_nn.values
        valores_baseline = np.zeros(valores_codificacion_nn.shape)

        rmse = Metricas.rmse(valores_codificacion_nn, valores_baseline)
        mse = Metricas.mse(valores_codificacion_nn, valores_baseline)
        return rmse, mse


    def obtener_rendimiento_baseline_popularidad(self) -> np.ndarray:
        valores_codificacion_nn = self.__df_codificacion_nn.values
        df_codificacion = self.__obtener_df_codificacion_sin_filtrar_ni_normalizar()
        df_baseline = self.__obtener_matriz_baseline_popularidad(
            df_codificacion)
        valores_baseline_pop = df_baseline.values

        rmse = Metricas.rmse(valores_codificacion_nn, valores_baseline_pop)
        mse = Metricas.mse(valores_codificacion_nn, valores_baseline_pop)
        return rmse, mse


    def __obtener_matriz_baseline_popularidad(self, df_codificacion:pd.DataFrame
                                              ) -> pd.DataFrame:
        interseccion_a_b = self.__obtener_interseccion_a_b_total()
        df_cod_filtrado_y = df_codificacion.loc[:, self.__elementos_y_seleccionados]
        # Intersección usuarios de a con cada poi
        pop_restaurante = df_cod_filtrado_y.sum(axis=0).values.reshape(1, -1)/interseccion_a_b
        pop_restaurante = np.array(pop_restaurante)
        # Como los totales por restaurante pueden ser 0, reemplazo estos por 1 
        # puesto que su numerados siempre va a ser 0
        pop_restaurante[pop_restaurante == 0] = 1

        df_cod_filtrado_y.loc[:, :] = df_cod_filtrado_y.values / pop_restaurante
        # downcast
        df_baseline_pop = df_cod_filtrado_y.loc[self.__elementos_x_seleccionados]
        return df_baseline_pop

    
    def __obtener_interseccion_a_b_total(self) -> int:
        ruta_f_intersecciones = self.__rutas.obtener_ruta_pipeline() + 'interseccion.txt'
        with open(ruta_f_intersecciones, 'r') as f: 
            interseccion_a_b = int(f.readline().split(';;')[0])
        return interseccion_a_b
    
    
    def __normalizar_codificacion(self, df_codificacion:pd.DataFrame, axis:int=1
                                  ) -> pd.DataFrame: 
        if axis == 0: # Normalización por columnas
            # Vector fila para hacer el cast
            vector_normalizacion = df_codificacion.values.sum(axis=0)
        elif axis == 1: # Normalización por filas
            # Vector columna para hacer el cast
            vector_normalizacion = df_codificacion.values.sum(axis=1).reshape(-1, 1) 
        else:
            raise ValueError('axis debe ser 1 o 0')
        vector_normalizacion[vector_normalizacion == 0] = 1 # Para evitar errores de div / 0
        df_codificacion_normalizada = df_codificacion / vector_normalizacion
        return df_codificacion_normalizada
    

    def __obtener_df_codificacion_sin_filtrar_ni_normalizar(self) -> pd.DataFrame:
        ruta_codificacion = self.__rutas.obtener_ruta_pipeline() + 'df_codificacion.parquet'
        df_codificacion = pd.read_parquet(ruta_codificacion)
        return df_codificacion