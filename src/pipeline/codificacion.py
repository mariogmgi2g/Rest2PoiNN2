import pandas as pd
import os
import pathlib
# nohup '../miniconda3/envs/tf/bin/python3' -u -m dev > 'data/pipeline/pivot.log' &


class Codificador:
    def __init__(self, ciudad_a:str, ciudad_b:str, tipo_a:str='restaurantes', tipo_b:str='pois') -> None:
        self.ciudad_a = ciudad_a
        self.tipo_a = tipo_a

        self.ciudad_b = ciudad_b
        self.tipo_b = tipo_b

        self.__comprobar_y_crear_ruta_codificacion()


    def generar_df_codificacion_a_partir_del_cod_tmp(self, eliminar_f_intersecciones_tmp:bool=False) -> None:
        """Función que lee el archivo temporal y lo procesa para generar un
        dataframe tabular de los elementos de la ciudad a en las filas, los
        elementos de la ciudad b en las columnas y la cantidad de usuarios 
        interseccionados como valores"""
        nombre_f_intersecciones_temporal = 'codificacion_tmp.txt'
        with open(self.__ruta_codificacion + nombre_f_intersecciones_temporal, 'r') as f:
            lista_vals = []
            cont = 0
            while True:
                linea = f.readline()
                linea = linea[:-1]
                if not linea: 
                    break
                # Formato de las líneas: contador;separador;ciudad_a;separador;ciudad_b;separador;interseccion
                linea_split = linea.split(';separador;')
                lista_vals.append( (linea_split[1], linea_split[2], int(linea_split[3])) )
                cont += 1

        df_unpivot = pd.DataFrame(lista_vals, columns=['restaurantes', 'pois', 'valores'])
        df_unpivot.drop_duplicates(inplace=True) #  No los quita, 

        df_unpivot = df_unpivot[~df_unpivot.duplicated(subset=['restaurantes', 'pois'], keep='first')]
        df_pivot = df_unpivot.pivot(index='restaurantes', columns='pois', values='valores')
        df_pivot = df_pivot.astype('Int64')

        recuento_celda_nulas = sum(df_pivot.isnull().values.ravel())
        print(f'Cantidad de celdas nulas: {recuento_celda_nulas}')

        ruta_pivot = self.__ruta_codificacion + 'df_codificacion.parquet'
        df_pivot.to_parquet(ruta_pivot)



    def __comprobar_y_crear_ruta_codificacion(self): # La misma que la de pipeline
        ruta_codificacion = f'./data/pipeline/{self.ciudad_a}_{self.tipo_a}__{self.ciudad_b}_{self.tipo_b}/'
        if not os.path.exists(ruta_codificacion):
            pathlib.Path(ruta_codificacion).mkdir(parents=True, exist_ok=True)
        self.__ruta_codificacion = ruta_codificacion