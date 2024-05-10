# pseudoclase en construcción
# variables globales 
import pandas as pd
import numpy as np
import json
import typing
import pathlib
import os
from concurrent import futures
import multiprocessing as mp
import requests


class Restaurante:
    def __init__(self, ciudad_id:int, ciudad_nombre:str, id:int, nombre:str, 
                 ref:str, num_aprox_reviews:int, reviews_por_pagina=15) -> None:
        # Asignación de atributos
        self.ciudad_id = ciudad_id
        self.ciudad_nombre = ciudad_nombre
        self.id = id
        self.nombre = nombre
        self.ref = ref
        self.num_aprox_reviews = num_aprox_reviews
        # atributo self.recuento_reviews_por_lang (pd.Series) ->
        self.reviews_por_pagina = reviews_por_pagina
        self.obtener_recuento_reviews_por_lang() 
        self.__comprobar_y_crear_ruta_reviews()
        self.__comprobar_y_crear_ruta_imagenes()
        

    
    def obtener_reviews(self, df_reviews_restaurante=pd.DataFrame()) -> int:
        # df_reviews_poi = pd.DataFrame() Está para lguardar los datos con 
        # seguridad y para paserle las reviews en caso de que deba continuar
        # DEVUELVE LA CANTIDAD DE REVIEWS DESCARGADAS
        ruta_reviews = self.__ruta_reviews + f'{self.id}_{self.nombre}.parquet'
        contador_reviews = 0

        flag_parada = False
        # Para cada uno de los langs
        for lang in self.recuento_reviews_por_lang.keys():
            # Se comprueba que no se estén retomando descargas parciales. 
            # No va a iniciar nunca una nueva descarga si ya se ha comenzado una,
            # va a intentar continuarla si lo considera adecuado.
            pagina = self.__comprobacion_es_retome(df_reviews_restaurante, lang)
            # Se sacan datos hasta que el df esté vacío
            print(f'{self.nombre} - {lang}:')
            while not flag_parada:
                df_pagina_reviews = self.obtener_reviews_de_pagina(pagina, lang)
                # Se para la extracción en el caso de que la página escrapeada
                # o bien esté vacía o bien ya tenga menos reviews de los 10 elementos
                print(f'*\tPoi {self.nombre}, lang {lang}: Descargando página {pagina}')
                df_reviews_restaurante = pd.concat([df_reviews_restaurante, df_pagina_reviews])
                contador_reviews += len(df_pagina_reviews)
                # Concatenado
                df_reviews_restaurante.reset_index(drop=True, inplace=True)
                # A veces no devuelve 10 reviews así que se le quita la condición del or

                # A veces hay páginas que están mal almacenadas en la fuente.
                # Es por ello que cada restaurante tiene un par de oportunidades
                # para saltar una página a pesar de que haya dado 0 reviews
                # siempre que todavía no se hayan descargado todas las reviews
                # para ese lang
                if df_pagina_reviews.empty:
                    reviews_que_objetivo_lang = self.recuento_reviews_por_lang[lang]
                    reviews_actuales_lang = len(self.reviews[self.reviews['lang'] == lang])
                    if reviews_que_objetivo_lang > reviews_actuales_lang and self.__oportunidades_saltarse_paginas > 0: # Si tiene oportunidades, se intenta saltar una página para ver si es que esa página está mal referenciada o hay un problema en la fuente
                        self.__oportunidades_saltarse_paginas -= 1
                    else: 
                        # Se da por vencida la extracción
                        flag_parada = True
                pagina += 1

                if pagina%7==0: # Se hace un guardado de seguridad cuando es múltiplo de 10 la página
                    df_reviews_restaurante.to_parquet(ruta_reviews)

            flag_parada = False
            # Lo guardamos siempre cuando se termina el scrapeo de ese lang adicionalmente
            df_reviews_restaurante.to_parquet(ruta_reviews)

        self.reviews = df_reviews_restaurante

        return contador_reviews


    def obtener_total_imagenes(self) -> int:
        # obtiene el número total de imágenes
        header = self.__obtener_cabecera(self.ref)
        payload = self.__obtener_payload_total_imagenes()
        data = requests.post(
            'https://www.tripadvisor.com/data/graphql/ids',
                json=payload,
                headers=header
            ).json()
        cantidad_fotos = data['data']['mediaAlbum']['album']['totalMediaCount']
        return cantidad_fotos

        
    def obtener_imagenes(self, flag_redescarga=False):
        # Ya tiene el listado de las refs descargadas durante las reviews
        ruta_listado_imgenes = self.__ruta_imagenes + 'listado_ref_imagenes.parquet'
        if os.path.exists(ruta_listado_imgenes):
            df_listado_ref_imagenes = pd.read_parquet(ruta_listado_imgenes)
            for fila in df_listado_ref_imagenes[['url', 'nombre']].values:
                url, nombre_imagen = fila
                ruta_imagen = self.__ruta_imagenes + nombre_imagen
                if not os.path.exists(ruta_imagen) or flag_redescarga:
                    try:
                        print(f'*\tDescargando imagen {nombre_imagen}')
                        img_data = requests.get(url, timeout=5).content
                        with open(ruta_imagen, 'wb') as handler:
                            handler.write(img_data)
                    except: # por timeout
                        print(f'Fallo en la descarga de la imagen {url}')
        else:
            print('El archivo listado_ref_imagenes.parquet no existe para este restaurante')

    
    def se_obtuvieron_todas_las_reviews_por_lang(self) -> dict: # Devuelve un diccionario con cada lang diciendo en cuáles si y en cuáles no
        # Nos aseguramos de que se tienen sus reviews de la mejor manera posible
        self.__comprobar_atributo_self_reviews()
        # Sacamos los langs de nuevo para evitar que el objeto haga el cálculo 
        # equivocado
        self.obtener_recuento_reviews_por_lang() 
        # Y lo comprobamos
        recuento_reviews_por_lang_de_descarga = self.reviews.groupby('lang')['usuario_ref'].count().to_dict()
        langs_a_continuar = {}
        for k in self.recuento_reviews_por_lang.keys():
            if k not in recuento_reviews_por_lang_de_descarga:
                print(k)
                langs_a_continuar[k] = self.recuento_reviews_por_lang[k]
            elif recuento_reviews_por_lang_de_descarga[k] < self.recuento_reviews_por_lang[k]:
                langs_a_continuar[k] = self.recuento_reviews_por_lang[k] - recuento_reviews_por_lang_de_descarga[k]

        return langs_a_continuar
    

    def se_descargo_correctamente(
            self, flag_continuar_descarga:bool=False) -> int:
        # Se asegura de que exista también self.reviews
        # DEVUELVE LA CANTIDAD DE NUEVAS REVIEWS o -1 SI NO HABÍA NUEVAS REVIEWS
        # PARA DESCARGAR
        langs_a_continuar = self.se_obtuvieron_todas_las_reviews_por_lang()
        self.recuento_reviews_por_lang = langs_a_continuar
        if len(self.recuento_reviews_por_lang) > 0:
            print(f'El restaurante {self.nombre} no se ha descargado completamente')
            # Solo se descargarán las necesarias porque se hace una comprobación 
            # de la página en la que termino cada lang y solo se mantienen los 
            # langs en el atributo que no hayan cumplido la cuota
            print(f'Los langs fallidos son {self.recuento_reviews_por_lang}')
            if flag_continuar_descarga:
                print('Se retoman las descargas...')
                contador_nuevas_reviews = self.obtener_reviews(df_reviews_restaurante=self.reviews)
            return contador_nuevas_reviews
        else:
            print(f'El restaurante {self.nombre} se ha descargado completamente')
            return -1
        

    def comprobar_reviews_paginas_descargadas(self):
        # Esta función evalúa las páginas que no tengan las 10 reviews que les
        # correspondiera, intentando una redescarga de las reviews y añadiendo
        # aquellas que todavía no estén
        # Se comprueba que el objeto tiene el atributo reviews y si no lo crea
        ruta_reviews = self.__ruta_reviews + f'{self.id}_{self.nombre}.parquet'
        self.__comprobar_atributo_self_reviews()
        # Se obtienen los langs que no se han conseguido descargar completamente
        langs_a_continuar = self.se_obtuvieron_todas_las_reviews_por_lang()
        self.recuento_reviews_por_lang = langs_a_continuar
        # Para cada uno de los langs, se hace un recuento de la cantidad de 
        # reviews por página, y se hacen consultas a aquellas que no tienen 10.
        # Luego, se mira si hay alguna que no estaba previamente en el conjunto
        # y se añade
        print('Se comprueban las páginas descargadas en busca de reviews faltantes...')
        for lang in self.recuento_reviews_por_lang:
            print(f'{self.nombre} - {lang}: ')
            serie_paginas = self.reviews.loc[self.reviews['lang'] == lang].groupby('pagina')['usuario'].count()
            paginas = serie_paginas[serie_paginas != self.reviews_por_pagina].index
            for pagina in paginas:
                print(f'*\t{self.nombre}, lang {lang}: Comprobando página {pagina}')
                ref_usuarios_actuales = self.reviews.loc[
                    (self.reviews['lang'] == lang) & (self.reviews['pagina'] == pagina), 
                    'usuario_ref']
                df_reviews_redescarga = self.obtener_reviews_de_pagina(pagina, lang)
                if not df_reviews_redescarga.empty:
                    filtro_antiguas = np.isin(df_reviews_redescarga['usuario_ref'], ref_usuarios_actuales)

                    reviews_nuevas = df_reviews_redescarga.loc[~filtro_antiguas]

                    self.reviews = pd.concat([self.reviews, reviews_nuevas])
                    self.reviews.reset_index(inplace=True, drop=True)
                    self.reviews.to_parquet(ruta_reviews)


    # -----------------
    def obtener_reviews_de_pagina(self, pagina:int, lang:str) -> pd.DataFrame:
        offset = pagina*self.reviews_por_pagina # Canitidad de eleementos por el número de páginas

        if pagina > 0:
            lista_split = self.ref.split('-')
            lista_split.insert(4, f'or{offset}')
            ref_pagina = '-'.join(lista_split)
        else:
            ref_pagina = self.ref

        # preparación de las atributos de la consulta
        header = self.__obtener_cabecera(ref_pagina)
        payload = self.__obtener_payload_reviews(offset, pagina, lang)

        data = requests.post(
        'https://www.tripadvisor.com/data/graphql/ids',
            json=payload,
            headers=header
        ).json()

        # Extracción de datos
        filas = []
        filas_fotos = []

        # por si diese un error puntual 
        oportunidades = 2
        falg_reintentar = True
        while oportunidades > 0 and falg_reintentar:
            try:
                restaurante_datos = data['data']['locations'][0] # ejemplo restaurante 0
                restaurante_reviews_por_langs = restaurante_datos['reviewAggregations']['languageCounts']
                restaurante_total_reviews = restaurante_datos['reviewListPage']['totalCount']
                restaurante_reviews = restaurante_datos['reviewListPage']['reviews']

                for i, review in enumerate(restaurante_reviews, 1): # Para cada una de las n reviews de la página...
                    dict_review = {}
                    # Si en la página no hay reviews entonces content tiene menos 
                    # elementos que el número de review se para para que no salte el error

                    if review['userProfile'] is None:
                        nombre_publico = 'eliminar'
                        url_perfil = 'eliminar'
                        ciudad_natal = None
                    else:
                        nombre_publico = review['userProfile']['displayName']
                        url_perfil = review['userProfile']['route']['url']
                        if review['userProfile']['hometown']['location'] is None:
                            ciudad_natal = None
                        else:
                            ciudad_natal = review['userProfile']['hometown']['location']['name']
                        
                    titulo = review['title']
                    texto = review['text']
                    fecha_publicacion_dtm = pd.to_datetime(review['publishedDate'])
                    review_puntuacion = review['rating']

                    # Asignación
                    dict_review['usuario'] = nombre_publico
                    dict_review['usuario_ref'] = url_perfil
                    dict_review['usuario_ciudad'] = ciudad_natal
                    dict_review['titulo'] = titulo
                    dict_review['texto'] = texto
                    dict_review['fecha publicacion'] = fecha_publicacion_dtm
                    dict_review['puntuacion'] = review_puntuacion
                    dict_review['pagina'] = pagina
                    dict_review['lang'] = lang

                    filas.append(dict_review)

                    fotos_review = [
                        {
                            'height': foto['photoSizes'][-1]['height'],
                            'width': foto['photoSizes'][-1]['width'],
                            'url': foto['photoSizes'][-1]['url']} 
                        for foto in review['photos']]
                    filas_fotos = filas_fotos + fotos_review

                    # Miro si debo guardar las keywords adicionales del poi
                    # if not hasattr(self, 'poi_keywords'):
                    #     self.restaurante_keywords = restaurante_keywords
                
                # combinación de los diccionarios en un dataframe
                df_reviews_pagina_restaurantes = pd.DataFrame(filas)
                
                # Se guaradan las fotos
                df_ref_fotos_parcial = pd.DataFrame(filas_fotos)
                self.__guardar_listado_reviews_pagina(df_ref_fotos_parcial)
                
                # retorno
                return df_reviews_pagina_restaurantes
            
            except Exception as e:
                print(e)

                falg_reintentar = True
                oportunidades -= 1

                
        # Se guaradan las fotos
        df_ref_fotos_parcial = pd.DataFrame(filas_fotos)
        self.__guardar_listado_reviews_pagina(df_ref_fotos_parcial)

        # retorno
        return pd.DataFrame()


    def obtener_recuento_reviews_por_lang(self) -> dict:
        # Extrae los grupos de filtrado de lenguage y su recuento de la consulta 
        # de la primera página de reviews. Los almacena en los atributos del 
        # objeto
        offset = 0
        ref_pagina = self.ref
        lang='en' # Ingés porque sería muy raro que al menos no haya una review
        pagina_inicial = 0

        # preparación de las atributos de la consulta
        header = self.__obtener_cabecera(ref_pagina)
        payload = self.__obtener_payload_reviews(offset, pagina_inicial, lang)

        data = requests.post(
        'https://www.tripadvisor.com/data/graphql/ids',
            json=payload,
            headers=header
        ).json()

        restaurante_datos = data['data']['locations'][0]
        dict_langs = restaurante_datos['reviewAggregations']['languageCounts']
        # Se elimina el total ya que nos interesan los langs pormenorizados
        self.recuento_reviews_por_lang = dict_langs
        return dict_langs


    def __guardar_listado_reviews_pagina(self, df_listado_fotos):
        if not df_listado_fotos.empty:
            # df_listado_fotos no tiene el nombre
            self.__comprobar_y_crear_ruta_imagenes()
            ruta_listado_ref_imagenes = self.__ruta_imagenes + 'listado_ref_imagenes.parquet'
            # se añade el nombre
            df_listado_fotos['nombre'] = df_listado_fotos['url'] \
                .map(lambda val: str(self.id) + '_' + val.split('/', 3)[3].replace('/', '_'))

            # Si ya está el listado se concatena
            if os.path.exists(ruta_listado_ref_imagenes):
                df_listado_fotos = pd.concat(
                    [pd.read_parquet(ruta_listado_ref_imagenes), df_listado_fotos])
                df_listado_fotos.drop_duplicates(inplace=True)

            df_listado_fotos.to_parquet(ruta_listado_ref_imagenes)


    def __comprobacion_es_retome(
            self, df_poi_reviews:pd.DataFrame, lang:str) -> int:
        # Devuelve la página si es retome, si no 0
        if not df_poi_reviews.empty: # Si no está vacío
            paginas = df_poi_reviews.loc[df_poi_reviews['lang'] == lang, 'pagina']
            if not paginas.empty: # Si el lang no está vacío
                return max(paginas) + 1
            return 0
        else:
            return 0


    # -----------------
    def __obtener_cabecera(self, ref_pagina:str) -> dict:
        # referencia_relativa tiene el formato /---.html
        headers = {
            'authority': 'www.tripadvisor.com',
            'accept': '*/*',
            'accept-language': 'es-ES,es;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'cookie': 'TASameSite=1; TAUnique=%1%enc%3ANCwvj5pjOh7dk0g9tT58cRhJpMB0YESwSzpLWlwO5QiRqDIW%2BjDBvQ%3D%3D; TASSK=enc%3AAMf5nMqsNhsa53lVQJCG59enytWiUVaq0UmEDxrIch0TU0VoQO2zAi1cB0CTD5VpjNTs2zUfDY9vsoNYutk6QklaxnRXUSMgBo59wRT2qMXQn6ZTUXd0sU1KiO4S8dOk5A%3D%3D; ServerPool=X; G_AUTH2_MIGRATION=informational; OptanonAlertBoxClosed=2023-11-08T14:53:00.074Z;  TATrkConsent=eyJvdXQiOiJTT0NJQUxfTUVESUEiLCJpbiI6IkFEVixBTkEsRlVOQ1RJT05BTCJ9; _ga=GA1.1.671870543.1699455183; eupubconsent-v2=CP0645gP0645gAcABBENDfCsAP_AAH_AACiQJrNX_T5eb2vi83ZcN7tkaYwP55y3o2wjhhaIEe0NwIOH7BoCJ2MwvBV4JiACGBAkkiKBAQVlHGBcCQAAgIgRiSKMYk2MjzNKJLJAilMbM0MYCD9mnsHT2ZCY70-uO7_zvneAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEDbACzBQuIAGwJCAmWjCCBAAMIwgKgBABQAJAUAGEIAICdgUBHrCQAAAEAAIAAAAAQRAAgAAEgCQiAAAAwEAgAAgEAAIABQgEABEgACwAtAAIABQDQkAooAhAMIMCECIUwIAgAAAAAAAAAAAAAAAAIBQIAYADoALgA2QB4AEQAMIAnQBcgDOAG2AO0AgcEAEAA6AFcARAAwgCdAGcAO0AgcGAEAA6AC4ANkAiABhAGcAO0AgcIAEAA6AGyARAAwgCdAGcAO0AgcKACABcAMIAzgCBwwAKAK4AwgDOAG2AQOHACAAdACuAIgAYQBOgDOAHaAQOIAAwDCAM4AgcSABgEQAMIBA4oAHAB0ARAAwgCdAGcAO0Agc.f_gAD_gAAAAA; TAReturnTo=%1%%2FRestaurant_Review-g187514-d2492660-Reviews-4D_Caffetteria-Madrid.html; VRMCID=%1%V1*id.10568*llp.%2F*e.1708969216155; pbjs_sharedId=f5c36b83-dcd9-4dec-9952-d859f3454495; pbjs_sharedId_cst=PSy8LAgsvw%3D%3D; AMZN-Token=v2FweIB5OHdycFdPS0gxTCs4UWlLN1Ztcng3amFTYU1oMDJ1Q1RldDFTZWU4cFE5a3R5MHQvYTF6RXM3a3N1cGswT2Q3bkFQVm5kTlJDenBkQi9WSXlpSGJNeVAxNHpUSHhJV0N5N3BUTFI2YlpnU3poSzZHL1FNc1ppMngvTjVLUG5mS2JrdgFiaXZ4IDJiaFpVKysvdmUrL3ZlKy92ZSsvdlh6dnY3ME5OQT09/w==; pbjs_unifiedID=%7B%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222024-04-01T15%3A21%3A04%22%7D; pbjs_unifiedID_cst=PSy8LAgsvw%3D%3D; _lr_env_src_ats=true; idl_env=AioQ3P2gYVjoVeMVaO4T3hfy-mrqEwpJul5egnmgv3vuNQCzh6tO9ROYKprb4PC29gLCWBHzp8ktJwJKG7R2Tz7k14F4IOp5w96I6eU1oY-H4_9UwHM72EFREv17j-vlwUFB; idl_env_cst=PSy8LAgsvw%3D%3D; idl_env_last=Mon%2C%2001%20Apr%202024%2015%3A21%3A05%20GMT; TAAUTHEAT=LQoRttjMg1xoVOe0ABQCNrrFLZA9QSOijcELs1dvVzyB3ryC-OcZ2pqsSg5BQNOU_nhOKQu-MOVng0GRsHM3SlPIyjkZj1q5Zp1LJ9DTL0DM6KKWo1JxMhU3b0ptmUoB0a4yWbvK-bwX6wvJqa0Y6Hmd5i9iFjWMP86dWtn4wCxqVonxTITt63BxEx2TzBknEDef3Z_ve9P5FMq4M8r_HRUNV0vJwJwEmlvv; _abck=754106B219FB596959791EFCF98236B3~-1~YAAQmbU+F1US3ZGOAQAAcaiKqQsmWqKhnY9CO6BgmDje4O0wMUs4RL4cKv/3Zaw8Bi/FizX/b2LyRGw+l0KldYYGkQwIupl66i5yt0NB6XnR3fgZKv6pukJQFndHJBOHPXoLfswnmRxePWDddrWvQ+utlD/jPK6xLhsNNu6iybnCci1FnuIeC8lgLznlyAUkN225ZrGrDgztMRevGvt0GujL4QIBAjPacqmeM/dXS+idMMLJTHaGUqCn+7eC64NUuQ67OQeDSOdoY7jMWKDHrGLI1H7jGX1q9AsnRctw7ut8SDgrjVEvW5QJ7C9nKEVngv2DWOMZTpvFnvJcZBeF5RfaO/Ea6/6J6xNjcgWqf9SujP/h72OfY8bCcFl5RNpge1LjrvT/yXlBVZta~-1~-1~-1; TADCID=MU5mMPa8KpEx6RD_ABQCmq6heh9ZSU2yA8SXn9Wv5Hyr4x9RXeBwxSKA_GER5ZiMkSzrM6MyTmfGEGMXM7HyBSeahrzhP2fZMWE; TASID=337C001CBB408ED35B918DA7C967D253; PAC=AJJhxBCAnYhhTj89yBe9fl-abm4Yc_V9Qxky27F6bqN7V4fWQo6kV1-prHNOamrrAukTOwpUXx62sVA6_irdTcGUFiJo2AgC83zz_r0Nzbr_IHmLjKhUcSOARHKPn3Rp1qmeiO_4P2VgDH7-0tjhHCW6cPhkFzrDW_KBWamBmKxM5ehtm0EKlGJXf75KFRE6DA%3D%3D; _lr_sampling_rate=0; PMC=V2*MS.86*MD.20240401*LD.20240404; TATravelInfo=V2*AY.2024*AM.4*AD.14*DY.2024*DM.4*DD.15*A.2*MG.-1*HP.2*FL.3*DSM.1712244307889*RS.1; roybatty=TNI1625\u0021AP2K2A7jwtUYJbnsYJZR5wzq1CadoPGpiK8b0ofJyVY9Nz%2FaHWGT4zLhVR2TSpwzfI9x3bjBRqUDmeTmlSP90N3IXpsghblCfM0TEMS40dUf00OyknAXwlVSCEkzahGhbDnJc9IuYg39ZShP7ofC9ay0WmW3YOySAOq1j4%2B2XA7F%2C1; TAUD=LA-1711984851593-1*ARC-1*RDD-2-2024_04_01*HDD-259456197-2024_04_14.2024_04_15*LD-259458658-2024.4.14.2024.4.15*LG-259458660-2.1.F.; SRT=TART_SYNC; TART=%1%enc%3A3ZNIPbU%2BfHFJcdS8R%2BEfVPEFH%2Bftz6d04qO%2B2CCsPBHOXIhS3MGB08ZV9hzv7wPyU6Kryyq1NcQ%3D; bm_sz=5911C387AD674B4799AD5A5C6DC4952E~YAAQB3R7XAQDJp6OAQAAryu4qRdnxxxeAbVl+jJCQu4ZAgBQqYITOq+qFbXY6xLpcELpCzl+nLbyU/oIIOc8Xx3uDgPF0wfxiTc44qguVsluB65y3/ngUbsbkTzWJ66lgQOyn+MDBDz2OtMO8zzhds/q++lqQk/0lxBzKhGgyCOQuz7f5scxDAyDMjJm1CcpApE0alw1PCHNtcGHPbbYbWtYeKFVrPSjn/HvTNSUB4jc0XavfA0yldh3Np4uZY/JJrdoVEL5z0Fw802Kbv+6NkqSbpYVtqWoHSdtc6abmP/D91w8YVarCMaf7IrD8giZWmcc1OTLrrTji97qTq2EBtyZFUJ1JNaktXopvDJFu6+8ksO2O1BeDJTPOw+C14wOWz6LBEnFobhlO2MXLQ35Rc5YtCQ9D5/5a6GgfG37WHFL28e96RPwM0PrkT/YatK0dcqDpJunxYc=~3355186~4601393; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Apr+04+2024+17%3A26%3A45+GMT%2B0200+(hora+de+verano+de+Europa+central)&version=202310.2.0&isIABGlobal=false&hosts=&consentId=5A4CC20591AEF0BE087A4220B9F66D57&interactionCount=36&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CV2STACK42%3A0&AwaitingReconsent=false&geolocation=ES%3BAS&browserGpcFlag=0; TASession=%1%V2ID.337C001CBB408ED35B918DA7C967D253*SQ.109*PR.40185%7C*LS.Attractions*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*TS.5A4CC20591AEF0BE087A4220B9F66D57*LF.en*FA.1*DF.0*TRA.false*LD.187791*EAU.%40; datadome=bnYLsPKKqJ9IqDPpw9tjBj9Tu_wvVrVeftowtE0PuDem0SOP2XY7mtlEVWz~ds4q9Yl7zY1K5iriPrGxR0MNvWgg5T7Cj26qnGkORakWmA4nemDNhN8v4wnajYquDMdx; __vt=2P7ngkln4WMH1YT8ABQCwRB1grfcRZKTnW7buAoPsSyJsUl4vw3sVb96-nQ8ANSMavY2cWopZKTPpPCJfGj6ANGL4MOs2BJ9_EAcn7HC_lvLiQardAE-DWlLHmlL_jBqzOLPMRD9ibpCheByPaoWTkhZBXtMFuC9ZAkWzQfAg5mYKOzFTPRa4BK0762KcfBU8k_Em0eGv7107gwMtQK4hSMLOpPeKdlQkQhZoQVEhsaScdJ4ljtXvvZsAQDMIRmF56YQmJR3gZNR7k9j_3WoyMveOlFuN8ZPPA22MSitS46NcQMSYU41ihO9Y7NYJNKK0KGYNlCOvBB0VitTMag7ELb3Hwpu8hoogecaXOE7; ab.storage.sessionId.6e55efa5-e689-47c3-a55b-e6d7515a6c5d=%7B%22g%22%3A%2218b66f1f-bc90-679f-b7b6-9a27535336b0%22%2C%22e%22%3A1712245401875%2C%22c%22%3A1712245386876%2C%22l%22%3A1712245386876%7D; ab.storage.deviceId.6e55efa5-e689-47c3-a55b-e6d7515a6c5d=%7B%22g%22%3A%226c58c1a4-cef1-d0ff-cdef-34deca815d93%22%2C%22c%22%3A1700650886400%2C%22l%22%3A1712245386880%7D; ab.storage.userId.6e55efa5-e689-47c3-a55b-e6d7515a6c5d=%7B%22g%22%3A%22MTA%3A5A4CC20591AEF0BE087A4220B9F66D57%22%2C%22c%22%3A1708364416598%2C%22l%22%3A1712245386890%7D; _ga_QX0Q50ZC9P=GS1.1.1712242005.13.1.1712245387.60.0.0',
            'origin': 'https://www.tripadvisor.com',
            'pragma': 'no-cache',
            'referer': f'https://www.tripadvisor.com' + ref_pagina,
            'sec-ch-device-memory': '8',
            'sec-ch-ua': 'Opera";v="105", "Chromium";v="119", "Not?A_Brand";v="24',
            'sec-ch-ua-arch': 'x86',
            'sec-ch-ua-full-version-list': 'Opera";v="105.0.4970.48", "Chromium";v="119.0.6045.199", "Not?A_Brand";v="24.0.0.0',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '',
            'sec-ch-ua-platform': 'macOS',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'same-origin',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0'
        }
        return headers


    def __obtener_payload_reviews(
            self, offset:int, pagina:int, lang:str # Páginas de 0 a n
            ) -> typing.List[dict]:
        payload = {
            "variables":{
                "locationId":self.id,
                "offset":offset,
                "limit":self.reviews_por_pagina,
                "keywordVariant":"location_keywords_v2_llr_order_30_en",
                "needKeywords":True,
                "userId":"5A4CC20591AEF0BE087A4220B9F66D57",
                "filters":[
                    {"axis":"SORT","selections":["mostRecent"]},
                    {"axis":"LANGUAGE","selections":[lang]}
                ],
                "prefs":{
                    "showMT":True,
                    "sortBy":"DATE",
                    "sortType":""
                },
                "initialPrefs":{
                    "showMT":True,
                    "sortBy":"DATE",
                    "sortType":""
                }
            },
            "extensions":{"preRegisteredQueryId":"452b31b03d0b13dd"}
        }
        return payload
    

    def __obtener_payload_imagenes_ref(self, n_elementos:int, offset:int) -> dict:
        print(f'Elementos: {n_elementos}')
        print(f'Offset: {offset}')
        payload_imagenes_ref = {
            "variables": {
                "locationId": self.id,
                "albumId": -160,
                "subAlbumId": -160,
                "client": "ar",
                "dataStrategy": "ar",
                "filter": {"mediaGroup": "ALL_INCLUDING_RESTRICTED"},
                "offset": offset,
                "limit": n_elementos
            },
                "extensions": {
                "preRegisteredQueryId": "2d46abde60a014b0"
            }
        }
        return payload_imagenes_ref


    def __obtener_payload_total_imagenes(self) -> dict:
        payload_imagenes_total = {
            "variables": {
                "locationId": self.id,
                "albumId": -160,
                "client": "ar",
                "dataStrategy": "ar",
                "filter": {"mediaGroup": "ALL_INCLUDING_RESTRICTED"},
                "subAlbumId": -160,
                "referenceMediaId": None
            },
            "extensions": {
                "preRegisteredQueryId": "2387a10aefe98942"
            }
        }
        return payload_imagenes_total
    

    # Otras funciones
    def __comprobar_y_crear_ruta_reviews(self):
        ruta_reviews = f'./data/scraping/restaurantes/{self.ciudad_nombre}/reviews/'
        if not os.path.exists(ruta_reviews):
            pathlib.Path(ruta_reviews).mkdir(parents=True, exist_ok=True)
        self.__ruta_reviews = ruta_reviews


    def __comprobar_y_crear_ruta_imagenes(self):
        ruta_imagenes = f'./data/scraping/restaurantes/{self.ciudad_nombre}/imagenes/{self.nombre}/'
        if not os.path.exists(ruta_imagenes):
            pathlib.Path(ruta_imagenes).mkdir(parents=True, exist_ok=True)
        self.__ruta_imagenes = ruta_imagenes
    

    def __comprobar_atributo_self_reviews(self) -> None:
        # Nos aseguramos de que se tienen sus reviews de la mejor manera posible
        if not hasattr(self, 'reviews'):
            # Si no se escrapearon las reviews tras la creación del objeto, se
            # leen las reviews almacenadas o se descargan si no existiese
            ruta_reviews = self.__ruta_reviews + f'{self.id}_{self.nombre}.parquet'
            if os.path.exists(ruta_reviews):
                self.reviews = pd.read_parquet(ruta_reviews)
            else:
                print('No se extrajeron las reviews de este restaurante')
                print('Procede a descargarse sus reviews...')
                self.obtener_reviews()