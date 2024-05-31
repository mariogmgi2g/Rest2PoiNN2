import numpy as np
import pandas as pd
from tensorflow import keras as k
from scipy.stats import kendalltau


class Metricas:
    @staticmethod
    def mse(y_true, y_pred) -> float:
        return np.average(np.average((y_pred - y_true)**2))
    

    @staticmethod
    def rmse(y_true, y_pred) -> float:
        return (Metricas.mse(y_true, y_pred))**0.5
    

    @staticmethod
    def tau_distancia(restaurantes_ordenados_true, restaurantes_ordenados_pred) -> float:
        # Los argumentos son dos listas, l
        # Conversion del coef de correlación tau kendall a distancia
        # porque cuando la correlación es máxima es 1, equivaldría a una 
        # distancia de 0, y cuando los rankings de cada lista son invertidos
        # la correlacion es de -1, que equivale a una distancia de 1
        tau_coef, p_valor = kendalltau(restaurantes_ordenados_true, restaurantes_ordenados_pred)
        tau_d = (1 - tau_coef) / 2
        return tau_d
    

    @staticmethod
    def obtener_metricas_rankings(
        df_y_true:pd.DataFrame, df_y_pred:pd.DataFrame, top:int, 
        axis_a_ordenar:int=0) -> dict:
        if axis_a_ordenar == 1: 
            df_y_true = df_y_true.T
            df_y_pred = df_y_pred.T

        tau_d_lista = []
        for col in df_y_true.columns:
            elementos_true_filas_ordenados_de_col = df_y_true.sort_values(
                by=col, ascending=False).iloc[:top].index
            elementos_pred_filas_ordenados_de_col = df_y_pred.sort_values(
                by=col, ascending=False).iloc[:top].index
            tau_d = Metricas.tau_distancia(
                elementos_true_filas_ordenados_de_col, 
                elementos_pred_filas_ordenados_de_col)
            tau_d_lista.append(tau_d)

        dict_resultado = {
            'Distancia Tau Kendall': {'media': np.mean(tau_d_lista), 'std': np.std(tau_d_lista)},
        }
        return dict_resultado