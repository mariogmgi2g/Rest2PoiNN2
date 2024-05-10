import pandas as pd
import numpy as np
import os
import pathlib
from itertools import product, islice
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
        ruta_cod_tmp = self.__ruta_pipeline + 'codificacion_tmp.txt'
        cont = self.__leer_ultimo_cont(ruta_cod_tmp)
        
        # se abre el documento si existe, Estructura: idx, elemento_a, elemento_b, interseccion
        with open(ruta_cod_tmp, 'a+') as f:  

            # Lectura de los elementos
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

            # cont = total_duplas - 30 # ELIMINAR TRAS ACABAR LAS COMPROBACIONES
            with Pool() as pool: # Por defecto, tantos subprocesos como cores
                resultados = pool.imap_unordered( # islice hace un slice del producto cartesiano
                    Pipeline.subproceso_codificacion, islice(producto_cartesiano, cont, total_duplas))
                
                for resultado in resultados:
                    interseccion, elemento_a, elemento_b = resultado
                    fila = f'{cont}, {elemento_a}, {elemento_b}, {interseccion}\n'
                    f.write(fila)
                    
                    print(f'Codificacion ({elemento_a}, {elemento_b}): {interseccion}, {cont}/{total_duplas} completadas')

                    cont += 1
                

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

    
    # def __leer_ultima_linea(ruta_archivo_tmp:str):
    def __leer_ultimo_cont(self, ruta_archivo_tmp:str):
        if os.path.exists(ruta_archivo_tmp):
            with open(ruta_archivo_tmp, 'rb') as f:
                try: # captura error en caso de que el archivo sea de una sola línea
                    f.seek(-2, os.SEEK_END)
                    while f.read(1) != b'\n':
                        f.seek(-2, os.SEEK_CUR)
                    ultima_linea = f.readline().decode()
                    cont = int(str(ultima_linea).split(', ')[0]) + 1
                    return cont
                except OSError:
                    f.seek(0)
                    return 0
                # last_line = f.readline().decode()
                # return last_line
        else: return 0