import pandas as pd
import numpy as np


class FiltroGenerador:
    # Sirve para filtrar los pois y restaurantes en base a sus características, 
    # especialmente los elementos de la ciudad B (es decir, los pois), 
    # en base a sus etiquetas
    def __init__(self) -> None:
        pass
    

    @staticmethod
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