import pandas as pd
import numpy as np
import os
import pathlib
from itertools import product
from multiprocessing import Pool
import pickle


class Pipeline:
    def __init__(self, ciudad_a:str, ciudad_b:str, tipo_a:str='restaurantes', tipo_b:str='pois') -> None:
        self.ciudad_a = ciudad_a
        self.tipo_a = tipo_a
        self.ciudad_b = ciudad_b
        self.tipo_b = tipo_b

        ruta_ciudad_a = f'./data/scraping/{tipo_a}/{ciudad_a}/'
        self.__ruta_ciudad_a = ruta_ciudad_a

        ruta_ciudad_b = f'./data/scraping/{tipo_b}/{ciudad_b}/'
        self.__ruta_ciudad_b = ruta_ciudad_b

        self.__comprobar_y_crear_ruta_pipeline()
        self.__ruta_interseccion = self.__ruta_pipeline + 'interseccion.txt'

    
    def obtener_usuarios_interseccionados(self, flag_reintento_guardado:bool=False) -> tuple:
        # Obtiene una tupla cuyo primer elemento es la cantidad de usuarios únicos
        # que hay en la intersección y el segundo una lista con los usuarios únicos
        # de la intersección.

        # Guarda un archivo de texto que tiene un primer número que indica la
        # cantidad de solapes seguido de un separador ";;" para continuar con 
        # cada uno de los usuarios solapados separados por ";"
        ruta_usuarios_por_elemento_a = self.__ruta_ciudad_a + 'usuarios_por_elemento.parquet'
        ruta_usuarios_por_elemento_b = self.__ruta_ciudad_b + 'usuarios_por_elemento.parquet'

        if flag_reintento_guardado or not os.path.exists(self.__ruta_interseccion):
            # Comprobacioes de que se han extraido los usuarios de cada una de 
            # las ciudades
            print('Generando archivo con las intersecciones en la ruta de la pipeline...')
            if not os.path.exists(ruta_usuarios_por_elemento_a):
                Pipeline.listado_usuarios_unicos_por_elemento(self.ciudad_a, self.tipo_a,
                    flag_redescarga=flag_reintento_guardado)
            if not os.path.exists(ruta_usuarios_por_elemento_b):
                Pipeline.listado_usuarios_unicos_por_elemento(self.ciudad_b, self.tipo_b,
                    flag_redescarga=flag_reintento_guardado)
            
            df_usuarios_ciudad_a = pd.read_parquet(ruta_usuarios_por_elemento_a)
            df_usuarios_ciudad_b = pd.read_parquet(ruta_usuarios_por_elemento_b)

            usuarios_unicos_a = df_usuarios_ciudad_a['usuario_ref'].unique()
            usuarios_unicos_b = df_usuarios_ciudad_b['usuario_ref'].unique()

            interseccion_bool = np.isin(usuarios_unicos_a, usuarios_unicos_b)
            usuarios_interseccion = usuarios_unicos_a[interseccion_bool]

            usuarios_encadenados = ';'.join(usuarios_interseccion)
            numero_usuarios = len(usuarios_interseccion)
            cadena = str(numero_usuarios) + ';;' + usuarios_encadenados

            with open(self.__ruta_interseccion, 'w+') as f:
                f.write(cadena)
            print('Archivo de intersecciones generado con éxito')
            
            return numero_usuarios, list(usuarios_interseccion)
        
        else:
            # formatea la tupla de numero de usuarios y usuarios unicos 
            # automáticamente desde el archivo
            return self.__leer_archivo_intersecciones()
    

    def obtener_codificacion_absoluta_multiproceso(self, flag_retomar=True) -> None:
        ruta_codificacion_pivot = self.__ruta_pipeline + 'codificacion_pivot.parquet'
        if flag_retomar and os.path.exists(ruta_codificacion_pivot):
            df_codificacion_pivot = pd.read_parquet(ruta_codificacion_pivot)
        else:
            df_codificacion_pivot = pd.DataFrame()

        ruta_elementos_a = f'./data/scraping/{self.tipo_a}/{self.ciudad_a}/listado_{self.tipo_a}.parquet'
        elementos_primitivo_a = pd.read_parquet(ruta_elementos_a, columns=['Id', 'nombre'])
        elementos_archivos_a = elementos_primitivo_a.apply(
            lambda val: f'./data/scraping/{self.tipo_a}/{self.ciudad_a}/reviews/' + '_'.join( [ str(val[0]), val[1] + '.parquet' ] ), axis=1).values

        ruta_elementos_b = f'./data/scraping/{self.tipo_b}/{self.ciudad_b}/listado_{self.tipo_b}.parquet'
        elementos_primitivo_b = pd.read_parquet(ruta_elementos_b, columns=['Id', 'nombre'])
        elementos_archivos_b = elementos_primitivo_b.apply(
            lambda val: f'./data/scraping/{self.tipo_b}/{self.ciudad_b}/reviews/' + '_'.join( [ str(val[0]), val[1] + '.parquet' ] ), axis=1).values

        total_duplas = len(elementos_archivos_a) * len(elementos_archivos_b)
        producto_cartesiano = product(elementos_archivos_a, elementos_archivos_b)

        with Pool() as pool: # Por defecto, tantos subprocesos como cores
            resultados = pool.imap_unordered(
                Pipeline.subproceso_codificacion, producto_cartesiano)
            
            for i, resultado in enumerate(resultados):
                interseccion, elemento_a, elemento_b = resultado
                fila = {'alemento_a': [elemento_a], 'alemento_b': [elemento_b], 'interseccion': [interseccion]}
                df_codificacion_pivot_fila = pd.DataFrame(fila)
                df_codificacion_pivot = pd.concat([ df_codificacion_pivot, df_codificacion_pivot_fila ])
                df_codificacion_pivot.to_parquet(ruta_codificacion_pivot)
                print(f'Codificacion ({elemento_a}, {elemento_b}): {interseccion}, {i}/{total_duplas} completadas')
        

    def obtener_codificacion_absoluta(
            self, flag_recalcular_usuarios_interseccionados:bool=False,
            ) -> None:
        # Genera un df cuyas columnas son los pois y las filas los restaurantes
        # y los valores el recuento de usuarios únicos interseccionados y lo guarda
        print('Calculando intersecciones cruzadas entre los elementos de las dos ciudades...')
        ruta_cod_absoluta = self.__ruta_pipeline + 'cod_absoluta.parquet'
        cantidad_usuarios, usuarios_unicos = self.obtener_usuarios_interseccionados(
            flag_reintento_guardado = flag_recalcular_usuarios_interseccionados
        )

        # Lectura de los archivos de listados de items
        ruta_df_usuarios_a = self.__ruta_ciudad_a + 'usuarios_por_elemento.parquet'
        ruta_df_usuarios_b = self.__ruta_ciudad_b + 'usuarios_por_elemento.parquet'

        df_usuarios_ciudad_a = pd.read_parquet(ruta_df_usuarios_a)
        df_usuarios_ciudad_b = pd.read_parquet(ruta_df_usuarios_b)


        # filas = {}
        dic_intersecciones = {}
        tipo_b_singular = self.tipo_b[:-1]
        print(f'Filas: {self.ciudad_a}, {self.tipo_a}')
        print(f'Columnas: {self.ciudad_b}, {self.tipo_b}')
        elementos_b_unicos = df_usuarios_ciudad_b['elemento'].unique()
        elementos_a_unicos = df_usuarios_ciudad_a['elemento'].unique()

        producto_cartesiano_elementos = product(elementos_a_unicos, elementos_b_unicos)
        n_combinaciones = len(elementos_b_unicos) * len(elementos_a_unicos)

        with Pool() as pool: # Por defecto, tantos subprocesos como cores
            resultados = pool.imap_unordered(
                Pipeline.__calcular_interseccion_usuarios, 
                producto_cartesiano_elementos)
            cont = 0
            for interseccion, elemento_a, elemento_b in resultados:
                dupla_elementos = (elemento_a, elemento_b)
                """
                dic_intersecciones[dupla_elementos] = interseccion
                with open(self.__ruta_pipeline + '.tmp_intersecciones.pkl', 'wb') as f:
                    pickle.dump(dic_intersecciones, f)
                """
                print(f'Se han calculado {cont}/{n_combinaciones} intersecciones')
                cont += 1

        """
        for i, elemento_b in enumerate(elementos_b_unicos): # pois
            print(f'*\t{tipo_b_singular} {elemento_b} ({i}/{len(elementos_b_unicos)})')
            fila = {}

            for elemento_a in elementos_a_unicos: # restaurantes
                
                usuarios_b = df_usuarios_ciudad_b.loc[
                    df_usuarios_ciudad_b['elemento'] == elemento_b, 
                    'usuario_ref'].values
                usuarios_a = df_usuarios_ciudad_a.loc[
                    df_usuarios_ciudad_a['elemento'] == elemento_a, 
                    'usuario_ref'].values
                fila[elemento_a] = sum(np.isin(usuarios_a, usuarios_b))

            filas[elemento_b] = fila

        print('Cálculo finalizado')
        df_cod_absoluta = pd.DataFrame(filas)
        df_cod_absoluta.to_parquet(ruta_cod_absoluta)
        """
                

    # Funciones Secundarias ----------------------------------------------------
    @staticmethod
    def listado_usuarios_unicos_por_elemento(
            ciudad:str, tipo_elemento:str, flag_redescarga:bool=False) -> None:
        # obtiene un df con estructura [elemento (poi/restaurante), usuario] 
        # y lo guardia en el directorio de la ciudad y su tipo_elemento
        ruta_ciudad = f'./data/scraping/{tipo_elemento}/{ciudad}/'
        ruta_df_usuarios = ruta_ciudad + 'usuarios_por_elemento.parquet'

        print(f'Descargando listado de usuarios únicos por elemento de {ciudad}, {tipo_elemento}')
        if flag_redescarga or not os.path.exists(ruta_df_usuarios):
            ruta_reviews = ruta_ciudad + 'reviews/'
            df_usuarios_por_elemento = pd.DataFrame()
            for elemento in os.listdir(ruta_reviews):
                df_reviews = pd.read_parquet(ruta_reviews + elemento)
                if not df_reviews.empty:
                    df_reviews['elemento'] = elemento[:-8]
                    df_usuarios_elemento = df_reviews[['elemento', 'usuario_ref']].drop_duplicates()
                    df_usuarios_por_elemento = pd.concat(
                        [df_usuarios_por_elemento, df_usuarios_elemento]) 
             
            df_usuarios_por_elemento.to_parquet(ruta_df_usuarios)
        print('Descarga finalizada')


    def subproceso_codificacion(tupla_valores:tuple) -> tuple:
        # Función para multiprocesado
        ruta_elemento_a, ruta_elemento_b = tupla_valores
        try:
            elemento_a = ruta_elemento_a.split('reviews/')[1].split('_', maxsplit=1)[1].replace('.parquet', '')
            elemento_b = ruta_elemento_b.split('reviews/')[1].split('_', maxsplit=1)[1].replace('.parquet', '')
        except Exception as e:
            print(elemento_a)
            print(elemento_a)
            raise e

        try:
            usuarios_a = pd.read_parquet(ruta_elemento_a, columns=['usuario_ref'])
            usuarios_unicos_a = usuarios_a['usuario_ref'].unique()
            usuarios_unicos_a = usuarios_unicos_a[usuarios_unicos_a != None]

            usuarios_b = pd.read_parquet(ruta_elemento_b, columns=['usuario_ref'])
            usuarios_unicos_b = usuarios_b['usuario_ref'].unique()
            usuarios_unicos_b = usuarios_unicos_b[usuarios_unicos_b != None]

            interseccion = sum(np.isin(usuarios_unicos_a, usuarios_unicos_b))
        except: 
            # puede suceder que no haya el parquet o que no tenga la columna de
            # usuarios_ref, con lo que se devolverá una intersección de 0
            interseccion = 0

        return interseccion, elemento_a, elemento_b


    def __leer_archivo_intersecciones(self) -> tuple:
        # Lee el archivo de la intersección y lo devuelve en forma de tupla
        with open(self.__ruta_interseccion, 'r') as f: cadena = f.read()
        cantidad_usuarios, usuarios_unicos = cadena.split(';;')
        cantidad_usuarios = int(cantidad_usuarios)
        usuarios_unicos = usuarios_unicos.split(';')
        return cantidad_usuarios, usuarios_unicos


    def __comprobar_y_crear_ruta_pipeline(self):
        ruta_pipeline = f'./data/pipeline/{self.ciudad_a}_{self.tipo_a}__{self.ciudad_b}_{self.tipo_b}/'
        if not os.path.exists(ruta_pipeline):
            pathlib.Path(ruta_pipeline).mkdir(parents=True, exist_ok=True)
        self.__ruta_pipeline = ruta_pipeline
