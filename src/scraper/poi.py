import pandas as pd
import numpy as np
import json
import typing
import pathlib
import os
import multiprocessing as mp
import requests


class Poi:
    def __init__(self, ciudad_id:int, ciudad_nombre:str, id:int, nombre:str, 
                 ref:str, num_aprox_reviews:int) -> None:
        # Asignación de atributos
        self.ciudad_id = ciudad_id
        self.ciudad_nombre = ciudad_nombre
        self.id = id
        self.nombre = nombre
        self.ref = ref
        self.num_aprox_reviews = num_aprox_reviews
        # atributo self.recuento_reviews_por_lang (pd.Series) ->
        self.obtener_recuento_reviews_por_lang() 
        self.__comprobar_y_crear_ruta_reviews()
        self.__comprobar_y_crear_ruta_imagenes()
        self.__oportunidades_saltarse_paginas = 2

    
    def obtener_reviews(self, df_reviews_poi=pd.DataFrame()) -> int:
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
            pagina = self.__comprobacion_es_retome(df_reviews_poi, lang)
            # Se sacan datos hasta que el df esté vacío
            print(f'{self.nombre} - {lang}:')
            while not flag_parada:
                df_pagina_reviews = self.obtener_reviews_de_pagina(pagina, lang)
                # Se para la extracción en el caso de que la página escrapeada
                # o bien esté vacía o bien ya tenga menos reviews de los 10 elementos
                print(f'*\tPoi {self.nombre}, lang {lang}: Descargando página {pagina}')
                df_reviews_poi = pd.concat([df_reviews_poi, df_pagina_reviews])
                contador_reviews += len(df_pagina_reviews)
                # Concatenado
                df_reviews_poi.reset_index(drop=True, inplace=True)
                # A veces no devuelve 10 reviews así que se le quita la condición del or

                # A veces hay páginas que están mal almacenadas en la fuente.
                # Es por ello que cada poi tiene un par de oportunidades
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
                    df_reviews_poi.to_parquet(ruta_reviews)

            flag_parada = False
            # Lo guardamos siempre cuando se termina el scrapeo de ese lang adicionalmente
            df_reviews_poi.to_parquet(ruta_reviews)

        self.reviews = df_reviews_poi

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
        self.__obtener_ref_imagenes(flag_redescarga=flag_redescarga)
        df_listado_ref_imagenes = pd.read_parquet(self.__ruta_imagenes + 'listado_ref_imagenes.parquet')
        for fila in df_listado_ref_imagenes[['url', 'nombre']].values:
            url, nombre_imagen = fila
            ruta_imagen = self.__ruta_imagenes + nombre_imagen
            if not os.path.exists(ruta_imagen):
                try:
                    img_data = requests.get(url, timeout=5).content
                    with open(ruta_imagen, 'wb') as handler:
                        handler.write(img_data)
                except: # por timeout
                    print(f'Fallo en la descarga de la imagen {url}')

    
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
            print(f'El poi {self.nombre} no se ha descargado completamente')
            # Solo se descargarán las necesarias porque se hace una comprobación 
            # de la página en la que termino cada lang y solo se mantienen los 
            # langs en el atributo que no hayan cumplido la cuota
            print(f'Los langs fallidos son {self.recuento_reviews_por_lang}')
            contador_nuevas_reviews = 0
            if flag_continuar_descarga:
                print('Se retoman las descargas...')
                contador_nuevas_reviews = self.obtener_reviews(df_reviews_poi=self.reviews)
            return contador_nuevas_reviews
        else:
            print(f'El poi {self.nombre} se ha descargado completamente')
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
            paginas = serie_paginas[serie_paginas != 10].index
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
        offset = pagina*10 # Canitidad de eleementos por el número de páginas

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

        # por si diese un error puntual 
        oportunidades = 2
        falg_reintentar = True
        while oportunidades > 0 and falg_reintentar:
            try:
                contenido_pagina = data[0]['data']['Result'][0]['detailSectionGroups'][-1]['detailSections'][0]['tabs'][0]['content']

                review_n = 1 # De 1 a 10, que es la cantidad de reviews por página

                for review_n in range(1, 11): # Para cada una de las 10 reviews de la página...
                    dict_review = {}
                    # Si en la página no hay reviews entonces content tiene menos 
                    # elementos que el número de review se para para que no salte el error
                    flag_no_hay_mas_reviews = len(contenido_pagina) <= review_n+1

                    if not flag_no_hay_mas_reviews:
                        if 'bubbleRatingNumber' in contenido_pagina[review_n+1]:
                            puntuacion = contenido_pagina[review_n+1]['bubbleRatingNumber']
                            keywords_poi = ';'.join(contenido_pagina[0]['keywords'])

                            texto = contenido_pagina[review_n+1]['htmlText']['text']
                            titulo = contenido_pagina[review_n+1]['htmlTitle']['text']
                            fecha_publicacion = contenido_pagina[review_n+1]['publishedDate']['text']
                            dic_perfil_usuario = contenido_pagina[review_n+1]['userProfile']

                            # formateo en datetime
                            fecha_publicacion_dtm = pd.to_datetime(
                                fecha_publicacion, format='Written %B %d, %Y')

                            ciudad_natal = dic_perfil_usuario['hometown']
                            nombre_publico = dic_perfil_usuario['displayName']
                            try:
                                url_perfil = dic_perfil_usuario['profileRoute']['url']
                            except:
                                url_perfil = None

                            # Asignación
                            dict_review['usuario'] = nombre_publico
                            dict_review['usuario_ref'] = url_perfil
                            dict_review['usuario_ciudad'] = ciudad_natal
                            dict_review['titulo'] = titulo
                            dict_review['texto'] = texto
                            dict_review['fecha publicacion'] = fecha_publicacion_dtm
                            dict_review['puntuacion'] = puntuacion
                            dict_review['pagina'] = pagina
                            dict_review['lang'] = lang

                            filas.append(dict_review)

                            # Miro si debo guardar las keywords adicionales del poi
                            if not hasattr(self, 'poi_keywords'):
                                self.poi_keywords = keywords_poi
                
                # combinación de los diccionarios en un dataframe
                df_reviews_pagina_pois = pd.DataFrame(filas)
                return df_reviews_pagina_pois
            
            except IndexError as e:
                es_error_puntual = data[0]['data']['Result'][0]['statusV2']['message'].startswith('Attraction not found')
                if es_error_puntual:
                    falg_reintentar = True
                    oportunidades -= 1
                else:
                    falg_reintentar = False
                    return pd.DataFrame()
            except Exception as e:
                # Distinguir en que nieguen la conexión y el Nonetype en alguna de las 
                # claves de data[0]['data']['Result'][0]['statusV2']['message'].startswith('Attraction not found')
                flag_reintentar = False
                return pd.DataFrame()

        return pd.DataFrame()


    def obtener_recuento_reviews_por_lang(self) -> None:
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

        info_langs = data[0]['data']['Result'][0]['detailSectionGroups'][-1] \
            ['detailSections'][0]['tabs'][0]['content'][0]['filterResponse']['availableFilterGroups'][1]['filter']['values']
        # Se extrae el recuento de todos los langs
        dict_langs = {dict_lang['value']: dict_lang['count'] for dict_lang in info_langs}
        # Se elimina el total ya que nos interesan los langs pormenorizados
        dict_langs.pop('all') 
        self.recuento_reviews_por_lang = dict_langs


    def obtener_ref_imagenes_de_pagina(self, pagina, elementos=50) -> pd.DataFrame:
        offset = elementos*pagina
        # elementos -> Número de imágenes por página
        header = self.__obtener_cabecera(self.ref)
        payload_imagenes_ref = self.__obtener_payload_imagenes_ref(elementos, offset)

        data = requests.post(
        'https://www.tripadvisor.com/data/graphql/ids',
            json=payload_imagenes_ref,
            headers=header
        ).json()
        # Se obtinen los diccionarios con las características {width, height, url}
        dicts_ref = [img['photoSizes'][-1] for img in data['data']['mediaAlbumPage']['mediaList']]
        df_ref_imagenes_pagina = pd.DataFrame(dicts_ref)
        # Se genera el nombre para guardarla más adelante
        df_ref_imagenes_pagina['nombre'] = df_ref_imagenes_pagina['url'] \
            .map(lambda val: str(self.id) + '_' + val.split('/', 3)[3].replace('/', '_'))
        return df_ref_imagenes_pagina


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


    def __obtener_ref_imagenes(self, elementos:int=50, flag_redescarga=False) -> None:
        # elementos -> Número de imágenes por página
        # Obtiene y guarda los links de todas las imágenes
        ruta_archivo = self.__ruta_imagenes + 'listado_ref_imagenes.parquet'
        if flag_redescarga or not os.path.exists(ruta_archivo):
            flag_parada = False
            pagina = 0
            df_ref_imagenes = pd.DataFrame()
            total_imagenes = self.obtener_total_imagenes()
            # reintentos por si no se descargase bien
            reintentos = 0
            max_reintentos = 3

            while not flag_parada:
                offset = pagina*elementos
                df_ref_imagenes_pagina = self.obtener_ref_imagenes_de_pagina(pagina)

                df_ref_imagenes = pd.concat([df_ref_imagenes, df_ref_imagenes_pagina])
                df_ref_imagenes.reset_index(drop=True, inplace=True)

                if (total_imagenes <= len(df_ref_imagenes)) \
                    or (df_ref_imagenes_pagina.empty and reintentos == max_reintentos):
                    flag_parada = True
                elif df_ref_imagenes_pagina.empty:
                    reintentos += 1
                else:
                    pagina += 1

            if total_imagenes > len(df_ref_imagenes):
                print(f'ATENCIÓN! Faltan urls en el listado de imágenes del poi {self.nombre} ({len(df_ref_imagenes)} de {total_imagenes})')
            df_ref_imagenes.to_parquet(ruta_archivo)


    # -----------------
    def __obtener_cabecera(self, ref_pagina:str) -> dict:
        # referencia_relativa tiene el formato /---.html
        headers = {
            'authority': 'www.tripadvisor.com',
            'accept': '*/*',
            'accept-language': 'es-ES,es;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'cookie': 'TASameSite=1; TAUnique=%1%enc%3ANCwvj5pjOh7dk0g9tT58cRhJpMB0YESwSzpLWlwO5QiRqDIW%2BjDBvQ%3D%3D; TASSK=enc%3AAMf5nMqsNhsa53lVQJCG59enytWiUVaq0UmEDxrIch0TU0VoQO2zAi1cB0CTD5VpjNTs2zUfDY9vsoNYutk6QklaxnRXUSMgBo59wRT2qMXQn6ZTUXd0sU1KiO4S8dOk5A%3D%3D; ServerPool=X; G_AUTH2_MIGRATION=informational; OptanonAlertBoxClosed=2023-11-08T14:53:00.074Z; OTAdditionalConsentString=1~43.46.55.61.70.83.89.93.108.117.122.124.135.136.143.144.147.149.159.192.196.202.211.228.230.239.259.266.286.291.311.317.320.322.323.327.338.367.371.385.394.397.407.413.415.424.430.436.445.453.482.486.491.494.495.522.523.540.550.559.560.568.574.576.584.587.591.737.802.803.820.821.839.864.899.904.922.931.938.979.981.985.1003.1027.1031.1040.1046.1051.1053.1067.1085.1092.1095.1097.1099.1107.1135.1143.1149.1152.1162.1166.1186.1188.1201.1205.1215.1226.1227.1230.1252.1268.1270.1276.1284.1290.1301.1307.1312.1345.1356.1364.1365.1375.1403.1415.1416.1421.1440.1449.1455.1495.1512.1516.1525.1540.1548.1555.1558.1570.1577.1579.1583.1584.1591.1603.1616.1638.1651.1653.1667.1677.1678.1682.1697.1699.1703.1712.1716.1721.1725.1732.1745.1750.1765.1769.1782.1786.1800.1810.1825.1827.1832.1838.1840.1842.1843.1845.1859.1866.1870.1878.1880.1889.1899.1917.1929.1942.1944.1962.1963.1964.1967.1968.1969.1978.2003.2007.2008.2027.2035.2039.2047.2052.2056.2064.2068.2072.2074.2088.2090.2103.2107.2109.2115.2124.2130.2133.2135.2137.2140.2145.2147.2150.2156.2166.2177.2183.2186.2205.2216.2219.2220.2222.2225.2234.2253.2279.2282.2292.2299.2305.2309.2312.2316.2322.2325.2328.2331.2334.2335.2336.2337.2343.2354.2357.2358.2359.2370.2376.2377.2387.2392.2400.2403.2405.2407.2411.2414.2416.2418.2425.2440.2447.2461.2462.2465.2468.2472.2477.2481.2484.2486.2488.2493.2498.2499.2501.2510.2517.2526.2527.2532.2535.2542.2552.2563.2564.2567.2568.2569.2571.2572.2575.2577.2583.2584.2596.2604.2605.2608.2609.2610.2612.2614.2621.2628.2629.2633.2636.2642.2643.2645.2646.2650.2651.2652.2656.2657.2658.2660.2661.2669.2670.2677.2681.2684.2687.2690.2695.2698.2713.2714.2729.2739.2767.2768.2770.2772.2784.2787.2791.2792.2798.2801.2805.2812.2813.2816.2817.2821.2822.2827.2830.2831.2834.2838.2839.2844.2846.2849.2850.2852.2854.2860.2862.2863.2865.2867.2869.2873.2874.2875.2876.2878.2880.2881.2882.2883.2884.2886.2887.2888.2889.2891.2893.2894.2895.2897.2898.2900.2901.2908.2909.2913.2914.2916.2917.2918.2919.2920.2922.2923.2927.2929.2930.2931.2940.2941.2947.2949.2950.2956.2958.2961.2963.2964.2965.2966.2968.2973.2975.2979.2980.2981.2983.2985.2986.2987.2994.2995.2997.2999.3000.3002.3003.3005.3008.3009.3010.3012.3016.3017.3018.3019.3024.3025.3028.3034.3037.3038.3043.3048.3052.3053.3055.3058.3059.3063.3066.3068.3070.3073.3074.3075.3076.3077.3078.3089.3090.3093.3094.3095.3097.3099.3104.3106.3109.3112.3117.3119.3126.3127.3128.3130.3135.3136.3145.3150.3151.3154.3155.3163.3167.3172.3173.3182.3183.3184.3185.3187.3188.3189.3190.3194.3196.3209.3210.3211.3214.3215.3217.3219.3222.3223.3225.3226.3227.3228.3230.3231.3234.3235.3236.3237.3238.3240.3244.3245.3250.3251.3253.3257.3260.3268.3270.3272.3281.3288.3290.3292.3293.3295.3296.3299.3300.3306.3307.3314.3315.3316.3318.3324.3327.3328.3330.3331.3531.3731.3831.3931.4131.4531.4631.4731.4831.5031.5231.6931.7031.7235.7831.7931.8931.9731.10231.10631.10831.11031.11531.12831.13632.13731.14237.16831; TATrkConsent=eyJvdXQiOiJTT0NJQUxfTUVESUEiLCJpbiI6IkFEVixBTkEsRlVOQ1RJT05BTCJ9; _ga=GA1.1.671870543.1699455183; eupubconsent-v2=CP0645gP0645gAcABBENDfCsAP_AAH_AACiQJrNX_T5eb2vi83ZcN7tkaYwP55y3o2wjhhaIEe0NwIOH7BoCJ2MwvBV4JiACGBAkkiKBAQVlHGBcCQAAgIgRiSKMYk2MjzNKJLJAilMbM0MYCD9mnsHT2ZCY70-uO7_zvneAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEDbACzBQuIAGwJCAmWjCCBAAMIwgKgBABQAJAUAGEIAICdgUBHrCQAAAEAAIAAAAAQRAAgAAEgCQiAAAAwEAgAAgEAAIABQgEABEgACwAtAAIABQDQkAooAhAMIMCECIUwIAgAAAAAAAAAAAAAAAAIBQIAYADoALgA2QB4AEQAMIAnQBcgDOAG2AO0AgcEAEAA6AFcARAAwgCdAGcAO0AgcGAEAA6AC4ANkAiABhAGcAO0AgcIAEAA6AGyARAAwgCdAGcAO0AgcKACABcAMIAzgCBwwAKAK4AwgDOAG2AQOHACAAdACuAIgAYQBOgDOAHaAQOIAAwDCAM4AgcSABgEQAMIBA4oAHAB0ARAAwgCdAGcAO0Agc.f_gAD_gAAAAA; TAReturnTo=%1%%2FRestaurant_Review-g187514-d2492660-Reviews-4D_Caffetteria-Madrid.html; VRMCID=%1%V1*id.10568*llp.%2F*e.1708969216155; pbjs_sharedId=f5c36b83-dcd9-4dec-9952-d859f3454495; pbjs_sharedId_cst=PSy8LAgsvw%3D%3D; AMZN-Token=v2FweIB5OHdycFdPS0gxTCs4UWlLN1Ztcng3amFTYU1oMDJ1Q1RldDFTZWU4cFE5a3R5MHQvYTF6RXM3a3N1cGswT2Q3bkFQVm5kTlJDenBkQi9WSXlpSGJNeVAxNHpUSHhJV0N5N3BUTFI2YlpnU3poSzZHL1FNc1ppMngvTjVLUG5mS2JrdgFiaXZ4IDJiaFpVKysvdmUrL3ZlKy92ZSsvdlh6dnY3ME5OQT09/w==; pbjs_unifiedID=%7B%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222024-04-01T15%3A21%3A04%22%7D; pbjs_unifiedID_cst=PSy8LAgsvw%3D%3D; _lr_env_src_ats=true; idl_env=AioQ3P2gYVjoVeMVaO4T3hfy-mrqEwpJul5egnmgv3vuNQCzh6tO9ROYKprb4PC29gLCWBHzp8ktJwJKG7R2Tz7k14F4IOp5w96I6eU1oY-H4_9UwHM72EFREv17j-vlwUFB; idl_env_cst=PSy8LAgsvw%3D%3D; idl_env_last=Mon%2C%2001%20Apr%202024%2015%3A21%3A05%20GMT; TAAUTHEAT=LQoRttjMg1xoVOe0ABQCNrrFLZA9QSOijcELs1dvVzyB3ryC-OcZ2pqsSg5BQNOU_nhOKQu-MOVng0GRsHM3SlPIyjkZj1q5Zp1LJ9DTL0DM6KKWo1JxMhU3b0ptmUoB0a4yWbvK-bwX6wvJqa0Y6Hmd5i9iFjWMP86dWtn4wCxqVonxTITt63BxEx2TzBknEDef3Z_ve9P5FMq4M8r_HRUNV0vJwJwEmlvv; _abck=754106B219FB596959791EFCF98236B3~-1~YAAQmbU+F1US3ZGOAQAAcaiKqQsmWqKhnY9CO6BgmDje4O0wMUs4RL4cKv/3Zaw8Bi/FizX/b2LyRGw+l0KldYYGkQwIupl66i5yt0NB6XnR3fgZKv6pukJQFndHJBOHPXoLfswnmRxePWDddrWvQ+utlD/jPK6xLhsNNu6iybnCci1FnuIeC8lgLznlyAUkN225ZrGrDgztMRevGvt0GujL4QIBAjPacqmeM/dXS+idMMLJTHaGUqCn+7eC64NUuQ67OQeDSOdoY7jMWKDHrGLI1H7jGX1q9AsnRctw7ut8SDgrjVEvW5QJ7C9nKEVngv2DWOMZTpvFnvJcZBeF5RfaO/Ea6/6J6xNjcgWqf9SujP/h72OfY8bCcFl5RNpge1LjrvT/yXlBVZta~-1~-1~-1; TADCID=MU5mMPa8KpEx6RD_ABQCmq6heh9ZSU2yA8SXn9Wv5Hyr4x9RXeBwxSKA_GER5ZiMkSzrM6MyTmfGEGMXM7HyBSeahrzhP2fZMWE; TASID=337C001CBB408ED35B918DA7C967D253; PAC=AJJhxBCAnYhhTj89yBe9fl-abm4Yc_V9Qxky27F6bqN7V4fWQo6kV1-prHNOamrrAukTOwpUXx62sVA6_irdTcGUFiJo2AgC83zz_r0Nzbr_IHmLjKhUcSOARHKPn3Rp1qmeiO_4P2VgDH7-0tjhHCW6cPhkFzrDW_KBWamBmKxM5ehtm0EKlGJXf75KFRE6DA%3D%3D; _lr_sampling_rate=0; PMC=V2*MS.86*MD.20240401*LD.20240404; TATravelInfo=V2*AY.2024*AM.4*AD.14*DY.2024*DM.4*DD.15*A.2*MG.-1*HP.2*FL.3*DSM.1712244307889*RS.1; roybatty=TNI1625\u0021AP2K2A7jwtUYJbnsYJZR5wzq1CadoPGpiK8b0ofJyVY9Nz%2FaHWGT4zLhVR2TSpwzfI9x3bjBRqUDmeTmlSP90N3IXpsghblCfM0TEMS40dUf00OyknAXwlVSCEkzahGhbDnJc9IuYg39ZShP7ofC9ay0WmW3YOySAOq1j4%2B2XA7F%2C1; TAUD=LA-1711984851593-1*ARC-1*RDD-2-2024_04_01*HDD-259456197-2024_04_14.2024_04_15*LD-259458658-2024.4.14.2024.4.15*LG-259458660-2.1.F.; SRT=TART_SYNC; TART=%1%enc%3A3ZNIPbU%2BfHFJcdS8R%2BEfVPEFH%2Bftz6d04qO%2B2CCsPBHOXIhS3MGB08ZV9hzv7wPyU6Kryyq1NcQ%3D; bm_sz=5911C387AD674B4799AD5A5C6DC4952E~YAAQB3R7XAQDJp6OAQAAryu4qRdnxxxeAbVl+jJCQu4ZAgBQqYITOq+qFbXY6xLpcELpCzl+nLbyU/oIIOc8Xx3uDgPF0wfxiTc44qguVsluB65y3/ngUbsbkTzWJ66lgQOyn+MDBDz2OtMO8zzhds/q++lqQk/0lxBzKhGgyCOQuz7f5scxDAyDMjJm1CcpApE0alw1PCHNtcGHPbbYbWtYeKFVrPSjn/HvTNSUB4jc0XavfA0yldh3Np4uZY/JJrdoVEL5z0Fw802Kbv+6NkqSbpYVtqWoHSdtc6abmP/D91w8YVarCMaf7IrD8giZWmcc1OTLrrTji97qTq2EBtyZFUJ1JNaktXopvDJFu6+8ksO2O1BeDJTPOw+C14wOWz6LBEnFobhlO2MXLQ35Rc5YtCQ9D5/5a6GgfG37WHFL28e96RPwM0PrkT/YatK0dcqDpJunxYc=~3355186~4601393; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Apr+04+2024+17%3A26%3A45+GMT%2B0200+(hora+de+verano+de+Europa+central)&version=202310.2.0&isIABGlobal=false&hosts=&consentId=5A4CC20591AEF0BE087A4220B9F66D57&interactionCount=36&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CV2STACK42%3A0&AwaitingReconsent=false&geolocation=ES%3BAS&browserGpcFlag=0; TASession=%1%V2ID.337C001CBB408ED35B918DA7C967D253*SQ.109*PR.40185%7C*LS.Attractions*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*TS.5A4CC20591AEF0BE087A4220B9F66D57*LF.en*FA.1*DF.0*TRA.false*LD.187791*EAU.%40; datadome=bnYLsPKKqJ9IqDPpw9tjBj9Tu_wvVrVeftowtE0PuDem0SOP2XY7mtlEVWz~ds4q9Yl7zY1K5iriPrGxR0MNvWgg5T7Cj26qnGkORakWmA4nemDNhN8v4wnajYquDMdx; __vt=2P7ngkln4WMH1YT8ABQCwRB1grfcRZKTnW7buAoPsSyJsUl4vw3sVb96-nQ8ANSMavY2cWopZKTPpPCJfGj6ANGL4MOs2BJ9_EAcn7HC_lvLiQardAE-DWlLHmlL_jBqzOLPMRD9ibpCheByPaoWTkhZBXtMFuC9ZAkWzQfAg5mYKOzFTPRa4BK0762KcfBU8k_Em0eGv7107gwMtQK4hSMLOpPeKdlQkQhZoQVEhsaScdJ4ljtXvvZsAQDMIRmF56YQmJR3gZNR7k9j_3WoyMveOlFuN8ZPPA22MSitS46NcQMSYU41ihO9Y7NYJNKK0KGYNlCOvBB0VitTMag7ELb3Hwpu8hoogecaXOE7; ab.storage.sessionId.6e55efa5-e689-47c3-a55b-e6d7515a6c5d=%7B%22g%22%3A%2218b66f1f-bc90-679f-b7b6-9a27535336b0%22%2C%22e%22%3A1712245401875%2C%22c%22%3A1712245386876%2C%22l%22%3A1712245386876%7D; ab.storage.deviceId.6e55efa5-e689-47c3-a55b-e6d7515a6c5d=%7B%22g%22%3A%226c58c1a4-cef1-d0ff-cdef-34deca815d93%22%2C%22c%22%3A1700650886400%2C%22l%22%3A1712245386880%7D; ab.storage.userId.6e55efa5-e689-47c3-a55b-e6d7515a6c5d=%7B%22g%22%3A%22MTA%3A5A4CC20591AEF0BE087A4220B9F66D57%22%2C%22c%22%3A1708364416598%2C%22l%22%3A1712245386890%7D; _ga_QX0Q50ZC9P=GS1.1.1712242005.13.1.1712245387.60.0.0',
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
        payload = [
            {
                "variables": {
                    "request": {
                        "tracking": {
                            "screenName": "Attraction_Review",
                            "pageviewUid": "LIT@lda7OF2@yAXzMZmwqfQy"
                        },
                        "routeParameters": {
                            "contentType": "attraction",
                            "contentId": str(self.id),
                            "pagee": str(offset)
                        },
                        "clientState": {
                            "userInput": [
                                {
                                "inputKey": "language",
                                "inputValues": [lang]
                                },
                            ]
                        }
                        # "clientState": None,
                    },
                    "commerce": {},
                    "tracking": {
                        "screenName": "Attraction_Review",
                        "pageviewUid": "LIT@lda7OF2@yAXzMZmwqfQy"
                    },
                    "currency": "USD",
                    "currentGeoPoint": None,
                    "unitLength": "MILES",
                    "pagee": str(offset),
                    "route": {
                        "page": "Attraction_Review",
                        "params": {
                        "geoId": self.ciudad_id,
                        "detailId": self.id,
                        "offset": f"r{offset}"
                        }
                    },
                    'currentPageNumber': str(pagina),
                    'limit': '10'
                },
                "extensions": {
                    "preRegisteredQueryId": "040e3c231ff050ca"
                }
            }
        ]
        return payload
    

    def __obtener_payload_imagenes_ref(self, n_elementos:int, offset:int) -> dict:
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
        ruta_reviews = f'./data/scraping/pois/{self.ciudad_nombre}/reviews/'
        if not os.path.exists(ruta_reviews):
            pathlib.Path(ruta_reviews).mkdir(parents=True, exist_ok=True)
        self.__ruta_reviews = ruta_reviews


    def __comprobar_y_crear_ruta_imagenes(self):
        ruta_imagenes = f'./data/scraping/pois/{self.ciudad_nombre}/imagenes/{self.nombre}/'
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
                print('No se extrajeron las reviews de este poi')
                print('Procede a descargarse sus reviews...')
                self.obtener_reviews()

"""
GUÍA DE LAS FUNCIONES MÁS IMPORTANTES-------------------------------------------
*   poi.obtener_reviews(df_reviews_poi(opcional)): Descarga las reviews del poi
        y las pone como atributo en self.reviews. Si hubiese un archivo que 
        correspondiese a las reviews descargadas del poi, lo carga automáticamente
        e intenta continuar la descarga.
*   poi.obtener_total_imagenes(): Devuelve la cantidad de imágenes que tiene el 
        poi
*   poi.se_obtuvieron_todas_las_reviews_por_lang(): Devuelve un diccionario con 
        los pares (lang, revies restantes para ese lang)
*   poi.obtener_imagenes(flag_redescarga=False): Descarga las imágenes del poi.
        Si la imagen existiera la redescarga si el flag está activo.
*   poi.se_descargo_correctamente(flag_continuar_descarga=False): Comprueba si 
        se descargaron correctamente todas las reviews devolviendo la cantidad
        de reviews faltantes devolviendo -1 si es que si, u otro valor si es que
        no. Este otra valor será 0 si el flag_continuar_descarga está desactivado,
        pero si el flag está activo intentará continuar la descarga y devolverá
        la cantidad de reviews descargadas durante la continuación. Por tanto, 
        si es 0 la cantidad de reviews continuadas, indicará que hay páginas en 
        alguno de los langas que está mal descargada y se deberá utilizar el 
        método poi.comprobar_reviews_paginas_descargadas().
*   poi.comprobar_reviews_paginas_descargadas(): comprueba por cada lang faltante
        qué páginas no tienen 10 reviews de las ya descargadas e intenta extraer 
        de nuevo solo las faltantes para evitar duplicados.
*   poi.obtener_reviews_de_pagina(pagina, lang): otiene un df con las reviews de
        una página de un lang.
*   poi.obtener_recuento_reviews_por_lang(): devuelve la cantidad de reviews por
        lang del poi.
*   poi.obtener_ref_imagenes_de_pagina(pagina, elementos=50): devuelve las 
        direcciones de "elementos" imágenes del poi de una página.
"""

"""
COLUMNAS DEL DF DE REVIEWS ({poi_id}_{poi_nombre}.parquet) ---------------------
*   usuario
*   usuario_ref
*   usuario_ciudad
*   titulo
*   texto
*   fecha publicacion
*   puntuacion
*   pagina
*   lang
"""

"""
EJEMPLO DE CONSTRUCTOR ---------------------------------------------------------
nombre = 'Trevi Fountain'
ciudad_id = 187791
ciudad_nombre = 'Rome_Lazio'
poi_id = 190131
num_aprox_reviews = 103792
ref = '/Attraction_Review-g187791-d190131-Reviews-Trevi_Fountain-Rome_Lazio.html'

poi = Poi(ciudad_id, ciudad_nombre, poi_id, nombre, ref, num_aprox_reviews)
intentos = 2
while intentos > 0 and flag_parada: 
    langs = poi.se_obtuvieron_todas_las_reviews_por_lang()
    cantidad_nuevas_reviews = poi.se_descargo_correctamente(flag_continuar_descarga=True)
    # Si se hubiese descargado completamente, la comprobación daría -1 en cantidad de nuevas reviews
    # Si da 0, es que hay reviews que descargar pero no ha sido capaz de sacarlas, 
    # con lo que se debería proceder a comprobar las páginas mal descargadas
    if cantidad_nuevas_reviews == 0: 
        poi.comprobar_reviews_paginas_descargadas()
    
    flag_parada = cantidad_nuevas_reviews == -1
    intentos -= 1   
"""

