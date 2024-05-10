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
from src.scraper.poi import Poi
# from poi import Poi


class PoisTripadvisor: # IMPLEMENTAR LANGS, SOLICITUD DE TOKENS, ALMACENAMIENTO Y MULTIHILO
    def __init__(self, ciudad_id:int, ciudad_nombre:str, elementos_por_pagina:int=30
                 ) -> None:
        self.ciudad_id = ciudad_id
        self.ciudad_nombre = ciudad_nombre
        self.__elementos_por_pagina = elementos_por_pagina
        self.__comprobar_y_crear_ruta_raiz()
        

    def obtener_listado_pois(self) -> pd.DataFrame:
        flag_parada = False

        # lista para guardar los dataframes resultantes y concatenarlos
        paginas_pois = []
        # mientras no se produzcan problemas continua haciendo scraping de las
        # paginas
        pagina = 0
        print('* Incio de la descarga de la lista de pois')
        while not flag_parada:
            print(f'\tDescargando pois de la página {pagina}')
            df_pagina_listado_pois = self.obtener_listado_pois_de_pagina(pagina)
            # Se sale del bucle si el df devuelto está vacío
            flag_parada = df_pagina_listado_pois.empty
    
            if not flag_parada:
                paginas_pois.append(df_pagina_listado_pois)
                pagina += 1

        # concatenación
        df_listado_pois = pd.concat(paginas_pois)
        df_listado_pois.reset_index(drop=True, inplace=True)
        df_listado_pois['Id'] = df_listado_pois['ref'].str.split('-')\
            .map(lambda lista: int(lista[2][1:]))
        
        # Guardado en datos -> scraping -> pois -> {ciudad}
        df_listado_pois.to_parquet(self.__ruta_raiz + 'listado_pois.parquet')
        self.listado_pois = df_listado_pois
                

    def descargar_informacion_pois(self):
        # Descarga con multithreating los parquets con las reviews de cada poi 
        # del listado
        # Se comprueba que existe el atributo self.listado_pois
        self.__comprobar_atributo_listado_pois()
        max_hilos = 30

        with futures.ThreadPoolExecutor(max_workers=max_hilos) as exe:
            hilos = [
                exe.submit(
                    PoisTripadvisor.__descargar_informacion_poi, # método
                    self.ciudad_id, self.ciudad_nombre, self.listado_pois.iloc[i, :]) # parámetros
                for i in range(len(self.listado_pois))
            ]

        # NO hay retorno de información si no se quieren analizar los pois
        # [hilo.result() for hilo in futures.as_completed(hilos)]


    def retomar_descarga_informacion_pois(self):
        # Descarga con multithreating los parquets con las reviews de cada poi, 
        # retomando los parquets ya descargados si existieran de cada uno de ellos
        # Se comprueba que existe el atributo self.listado_pois
        self.__comprobar_atributo_listado_pois()
        max_hilos = 30


        with futures.ThreadPoolExecutor(max_workers=max_hilos) as exe:
            hilos = [
                exe.submit(
                    PoisTripadvisor.__comprobar_descarga_reviews_poi, # método
                    self.ciudad_id, self.ciudad_nombre, self.listado_pois.iloc[i, :]) # parámetros
                for i in range(len(self.listado_pois))
            ]

        # NO hay retorno de información si no se quieren analizar los pois
        # [hilo.result() for hilo in futures.as_completed(hilos)]


    def descargar_imagenes_pois(self):
        self.__comprobar_atributo_listado_pois()
        max_hilos = 30

        with futures.ThreadPoolExecutor(max_workers=max_hilos) as exe:
            hilos = [
                exe.submit(
                    PoisTripadvisor.__descargar_imagenes_poi, # método
                    self.ciudad_id, self.ciudad_nombre, self.listado_pois.iloc[i, :]) # parámetros
                for i in range(len(self.listado_pois))
            ]

    # ------------ Funciones complemento de obtener_listado_pois() ----------- #
    def obtener_listado_pois_de_pagina(self, pagina:int) -> pd.DataFrame:
        # Construcción de cabecera y payload
        num_elemento_inicial = pagina*self.__elementos_por_pagina
        # referencia relatuva para completar la cabecera
        referencia_relativa = f'/Attractions-g{self.ciudad_id}-Activities-oa{num_elemento_inicial}-{self.ciudad_nombre}.html'
        cabecera = self.__obtener_cabecera(referencia_relativa)
        payload = self.__obtener_payload_listado_pois(num_elemento_inicial)

        # Petición a la graph sql para obtener la info raw
        data = requests.post(
        'https://www.tripadvisor.com/data/graphql/ids',
            json=payload,
            headers=cabecera
        ).json()

        # Navegación por la respuesta
        navegacion = data[9]['data']['Result'][0]['sections']
        filas = []
        # watcher como contador para comprobar que el orden de los pois es el 
        # correcto y que se estan descargando de forma adecuada
        watcher_poi_actual = num_elemento_inicial

        # Bucle dentro de la página de listado de pois para encontrarlos pois
        for i, dict_val in enumerate(navegacion):
            dict_poi = {}
            try:
                navegacion2 = dict_val['singleFlexCardContent']
                # poi numero
                prefijo_ordinal = navegacion2['ordinalPrefix']
                poi_numero = int(prefijo_ordinal.replace('.', ''))
                # poi nombre
                poi_nombre = navegacion2['cardTitle']['text']
                # poi ref
                poi_ref = navegacion2['cardLink']['webRoute']['webLinkUrl']
                # poi tags - tamaño variable así que van separadas por ;
                lista_tag = navegacion2['tagIdInfo']
                if navegacion2['bubbleRating'] != None:
                    # poi puntuacion
                    poi_puntuacion = navegacion2['bubbleRating']['rating']
                    # poi numero reviews
                    poi_cantidad_reviews = navegacion2['bubbleRating']['numberReviews']['text']
                    # El formato tiene una separación de comas por millares, con lo 
                    # que lo elimino y lo paso a int
                    poi_cantidad_reviews = int(poi_cantidad_reviews.replace(',', ''))
                else:
                    poi_puntuacion = 0
                    poi_cantidad_reviews = 0

                # Se comprueba que el numero es el adecuado y que no está
                # saltándose páginas
                assert(watcher_poi_actual == poi_numero, 'El watcher indica que no es la página correcta')

                # asignación
                dict_poi['numero'] = poi_numero
                dict_poi['nombre'] = poi_nombre
                dict_poi['puntuacion'] = poi_puntuacion
                dict_poi['cantidad reviews'] = poi_cantidad_reviews
                dict_poi['ref'] = poi_ref
                dict_poi['tags'] = ';'.join(
                    [tag['text']['text'] for tag in lista_tag])

                filas.append(dict_poi)
                
                watcher_poi_actual += 1
            except KeyError as e: 
                # Si el error es porque no existe 'singleFlexCardContent' es 
                # porque no es el elemento que estoy buscando. Si fuera otro 
                # probablemente tenga que ver con un bloqueo o mal 
                # funcionamiento del programa
                if e.args[0] == 'singleFlexCardContent': pass
            except Exception as e:
                print(filas)
                print(navegacion2)
                raise e
        
        # combinación de los diccionarios en un dataframe
        df_listado_pagina_pois = pd.DataFrame(filas)
        return df_listado_pagina_pois


    # ------------------------- Headers & Paylodas --------------------------- #
    # Constructores peteciones http
    def __obtener_cabecera(self, referencia_relativa:str) -> dict:
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
            'referer': f'https://www.tripadvisor.com' + referencia_relativa,
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


    def __obtener_payload_listado_pois(self, num_elemento_inicial:int
                                       ) -> typing.List[dict]:
        payload = [
            { 
                # pagee puede que se la pagina pura y no el elemento inicial
                "variables":{
                    "navigations":{
                        "clientRequestTimestampMs":1712245386696,
                        "request":[
                            {
                                "eventTimestampMs":1712245386696,
                                "fromPage":"AttractionsFusion",
                                "fromParams":[
                                    {"key":"pagee","value":str(num_elemento_inicial)}, # pagee 0
                                    {"key":"geoId","value":str(self.ciudad_id)},
                                    {"key":"contentType","value":"attraction"},
                                    {"key":"webVariant","value":"AttractionsFusion"}
                                ],
                                "fromPath":f"/Attractions-g{self.ciudad_id}-Activities-oa{num_elemento_inicial}-{self.ciudad_nombre}.html", # pagee que es exactamente??
                                # pagee 0 abajo
                                "fromRoute":'{"page":"AttractionsFusion","params":{"pagee":"' + str(num_elemento_inicial) + '","geoId":'+ str(self.ciudad_id) + ',"contentType":"attraction","webVariant":"AttractionsFusion"},"path":"/Attractions-g'+ str(self.ciudad_id) + '-Activities-oa' + str(num_elemento_inicial) + '-' + self.ciudad_nombre + '.html","fragment":""}',
                                "identifierType":"TA_PERSISTENTCOOKIE",
                                "navigationType":"USER_INITIATED",
                                "opaqueIds":[{"key":"MEMBER_UID","value":"5A4CC20591AEF0BE087A4220B9F66D57"}],
                                "origin":"https://www.tripadvisor.com",
                                "referrer":f"https://www.tripadvisor.com/Attractions-g{self.ciudad_id}-Activities-oa{num_elemento_inicial}-{self.ciudad_nombre}.html",
                                "toPage":"AttractionsFusion",
                                "toParams":[
                                    {"key":"pagee","value":str(num_elemento_inicial)}, # Aqui og = 30
                                    {"key":"geoId","value":str(self.ciudad_id)},
                                    {"key":"contentType","value":"attraction"},
                                    {"key":"webVariant","value":"AttractionsFusion"},
                                    {"key":"sort","value":"undefined"},
                                    {"key":"filters","value":"[]"}
                                ],
                                "toPath":f"/Attractions-g{self.ciudad_id}-Activities-oa{str(num_elemento_inicial)}-{self.ciudad_nombre}.html",
                                "toRoute":'{"page":"AttractionsFusion","params":{"pagee":"' + str(num_elemento_inicial) + '","geoId":'+ str(self.ciudad_id) + ',"contentType":"attraction","webVariant":"AttractionsFusion","filters":[]},"path":"/Attractions-g'+ str(self.ciudad_id) + '-Activities-oa' + str(num_elemento_inicial) + '-' + self.ciudad_nombre + '.html","fragment":""}',
                                "uid":"69d50522-3933-4262-84f9-a4002672dd75",
                                "userAgent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0"
                            }
                        ]
                    }
                },
                "extensions":{"preRegisteredQueryId":"8f5c28f35caeff98"}
            },
            {
                "variables":{
                    "events":[
                        {
                            "schemaName":"page_viewed__2",
                            "eventJson":'{"producer_ref":"ta-web-domain","event":"page_viewed","page":{"name":"AttractionsFusion","locale":"en-US","tld":"com","uid":"LIT@6OUPKRfoS@MqyNbjbgRL"},"event_source":{"brand":"TA","governance":{"domain":"Unknown"}},"event_type":{"name":"Page Viewed","version":2},"geo":{"id":'+ str(self.ciudad_id) + '},"route":{"page":"AttractionsFusion","params":{"pagee":"' + str(num_elemento_inicial) + '","geoId":"'+ str(self.ciudad_id) + '","contentType":"attraction","webVariant":"AttractionsFusion","sort":"undefined","filters":""},"path":"/Attractions-'+ str(self.ciudad_id) + '-Activities-oa' + str(num_elemento_inicial) + '-' + self.ciudad_nombre + '.html","fragment":""},"filter":{"hotels":{"rooms":1,"dates":{"start":"2024-04-14","end":"2024-04-15","source":"auto","length_in_days":1,"day_of_week_of_start":"Sun","day_of_week_of_end":"Mon"},"party":{"total":2,"adults":2,"children":0}},"experiences":{"party":{"total":0,"adults":0,"children":0}},"vacation_rentals":{"dates":{"start":"2024-04-14","end":"2024-04-15","source":"user","length_in_days":1,"day_of_week_of_start":"Sun","day_of_week_of_end":"Mon"},"party":{"total":2,"adults":2,"children":0}},"flights":{"class_of_service":"ECONOMY","is_one_way":true,"dates":{"start":"2024-04-18","end":"2024-04-25","source":"user","length_in_days":7,"day_of_week_of_start":"Thu","day_of_week_of_end":"Thu"},"party":{"total":1,"adults":1,"children":0,"seniors":0}}},"path":"/Attractions-g'+ str(self.ciudad_id) + '-Activities-oa' + str(num_elemento_inicial) + '-' + self.ciudad_nombre + '.html","referrer":"https://www.tripadvisor.com/Attractions-g'+ str(self.ciudad_id) + '-Activities-oa' + str(num_elemento_inicial) + '-' + self.ciudad_nombre + '.html","request":{"locale":"en-US","session":"337c001c-bb40-8ed3-5b91-8da7c967d253","user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 OPR/105.0.0.0"},"client_timestamp":"2024-04-04T15:43:06.846Z","user_device":{"os":"unknown","kind":"desktop","app_kind":"web","browser":"chrome","browser_version":"119"},"identifiers":{},"consent":{"ta":{}}}'
                        }
                    ]
                },
                "extensions":{"preRegisteredQueryId":"636d0b9184b2fc29"}
            },
            {
                "variables":{
                    "page":"AttractionsFusion",
                    "pos":"en-US",
                    "parameters":[
                        {"key":"pagee","value":str(num_elemento_inicial)},
                        {"key":"geoId","value":str(self.ciudad_id)},
                        {"key":"contentType","value":"attraction"},
                        {"key":"webVariant","value":"AttractionsFusion"},
                        {"key":"sort","value":"undefined"},
                        {"key":"filters","value":""}
                    ],
                    "factors":["TITLE","META_DESCRIPTION","MASTHEAD_H1","MAIN_H1","IS_INDEXABLE","RELCANONICAL"],
                    "route":{
                        "page":"AttractionsFusion",
                        "params":{
                            "pagee":str(num_elemento_inicial),
                            "geoId":self.ciudad_id,
                            "contentType":"attraction",
                            "webVariant":"AttractionsFusion",
                            "filters":[]
                        }
                    }
                },
                "extensions":{"preRegisteredQueryId":"8ff5481f70241137"}
            },
            {
                "variables":{
                    "uid":"LIT@6OUPKRfoS@MqyNbjbgRL",
                    "sessionId":"337C001CBB408ED35B918DA7C967D253",
                    "currency":"USD",
                    "sessionType":"DESKTOP",
                    "locationId":self.ciudad_id,
                    "page":"AttractionsFusion"
                },
                "extensions":{"preRegisteredQueryId":"fa5da6ee0b1deed3"}
            },
            {
                "variables":{
                    "locationId":self.ciudad_id,
                    "uid":"LIT@6OUPKRfoS@MqyNbjbgRL",
                    "sessionId":"337C001CBB408ED35B918DA7C967D253",
                    "currency":"USD"
                },
                "extensions":{"preRegisteredQueryId":"42bec0ee6ec0bfd1"}
            },
            { 
                "variables":{ # Aqúi el oa es 0 también, pero el pagee es 30
                    "pageName":"AttractionsFusion",
                    "relativeUrl":f"/Attractions-g{self.ciudad_id}-Activities-oa{num_elemento_inicial}-{self.ciudad_nombre}.html",
                    "parameters":[
                        {"key":"pagee","value":str(num_elemento_inicial)},
                        {"key":"geoId","value":str(self.ciudad_id)},
                        {"key":"contentType","value":"attraction"},
                        {"key":"webVariant","value":"AttractionsFusion"},
                        {"key":"sort","value":"undefined"},
                        {"key":"filters","value":""}
                    ],
                    "route":{
                        "page":"AttractionsFusion",
                        "params":{
                            "pagee":str(num_elemento_inicial),
                            "geoId":self.ciudad_id,
                            "contentType":"attraction",
                            "webVariant":"AttractionsFusion",
                            "filters":[]
                        }
                    }
                },
                "extensions":{"preRegisteredQueryId":"1a7ccb2489381df5"}
            },
            {
                "variables":{
                    "page":"AttractionsFusion",
                    "params":[
                        {"key":"pagee","value":str(num_elemento_inicial)},
                        {"key":"geoId","value":str(self.ciudad_id)},
                        {"key":"contentType","value":"attraction"},
                        {"key":"webVariant","value":"AttractionsFusion"},
                        {"key":"sort","value":"undefined"},
                        {"key":"filters","value":""}
                    ],
                    "route":{
                        "page":"AttractionsFusion",
                        "params":{
                            "pagee":str(num_elemento_inicial),
                            "geoId":self.ciudad_id,
                            "contentType":"attraction",
                            "webVariant":"AttractionsFusion",
                            "filters":[]
                        }
                    }
                },
                "extensions":{"preRegisteredQueryId":"f742095592a84542"}
            },
            {
                "variables":{
                    "params":{
                        "pagee":str(num_elemento_inicial),
                        "geoId":self.ciudad_id,
                        "contentType":"attraction",
                        "webVariant":"AttractionsFusion",
                        "filters":[]
                    },
                    "page":"AttractionsFusion",
                    "fragment":""
                },
                "extensions":{"preRegisteredQueryId":"a26bffd43d0e25b6"}
            },
            {
                "variables":{ # Estos dos números igual es porque varias localizaciones se asignan a un área metropolitana grande
                    "request":[{"locationIds":[self.ciudad_id, self.ciudad_id-2],"pageContentId":None,"pageType":None,"timestamp":"1712245386717"}]
                },
                "extensions":{"preRegisteredQueryId":"f6368397e59fe429"}
            },
            {
                "variables":{
                    "request":{
                        "tracking":{
                            "screenName":"AttractionsFusion",
                            "pageviewUid":"LIT@6OUPKRfoS@MqyNbjbgRL"
                        },
                        "routeParameters":{
                            "geoId":self.ciudad_id,
                            "pagee":str(num_elemento_inicial),
                            "contentType":"attraction",
                            "webVariant":"AttractionsFusion",
                            "filters":[]
                        },
                        "updateToken":None
                    },
                    "commerce":{"attractionCommerce":{"pax":[{"ageBand":"ADULT","count":2}]}},
                    "tracking":{"screenName":"AttractionsFusion","pageviewUid":"LIT@6OUPKRfoS@MqyNbjbgRL"},
                    "sessionId":"337C001CBB408ED35B918DA7C967D253",
                    "unitLength":"MILES",
                    "currency":"USD",
                    "currentGeoPoint":None,
                    "sectionTypes":["Mixer_ArticlesHeroStoriesHighlightSection","Mixer_ArticlesMosaicSection","Mixer_FeaturedStoriesSection","Mixer_Shelf","Mixer_EditorialFeatureSection","Mixer_FullImageFeatureCardSection","Mixer_InsetImageFeatureCardSection","Mixer_CoverPageHeroSection","Mixer_AdPlaceholderSection","Mixer_CategorySearchesSection","Mixer_FactSheetSection","Mixer_PromotionalBannerSection","Mixer_TravelersChoiceSection","Mixer_InteractiveMapSection","Mixer_ArticlesKeepExploringSection","Mixer_AsFeaturedInWidgetSection","Mixer_VideoPlayerSection","Mixer_ReviewExcerptSection","Mixer_BrandChannelCommerceSection","Mixer_PlanYourTripSection","Mixer_BuildTripWithAIHomeSection","Mixer_LocalGuidesSection","Mixer_SponsoredTourismSection","Mixer_CollectionShelfSection"],
                    "sectionContentTypes":["Mixer_DescriptionAndCarousel","Mixer_FlexGrid","Mixer_ImageAndCarousel","Mixer_MediumCarousel","Mixer_NarrowCarousel","Mixer_PlusCarousel","Mixer_TravelerSpotlightCarousel","Mixer_WideCarousel","Mixer_ProminentFlexList"],
                    "cardTypes":["Mixer_PoiVerticalStandardCard","Mixer_PoiVerticalMerchandisingCard","Mixer_PoiVerticalDescriptionCard","Mixer_PoiVerticalNameWithButtonCard","Mixer_GeoVerticalMinimalCard","Mixer_TripVerticalContributorCard","Mixer_VrGeoVerticalMinimalCard","Mixer_GeoImageBackgroundCard","Mixer_CustomImageBackgroundCard","Mixer_AttractionTaxonomyImageBackgroundCard","Mixer_LinkPostEditorialCard","Mixer_TripEditorialCard","Mixer_VideoEditorialCard","Mixer_ForumCard","Mixer_UgcEditorialFeatureLinkPostCard","Mixer_UgcEditorialFeatureTripCard","Mixer_ReviewVerticalContributorCard","Mixer_CustomVerticalMinimalCard","Mixer_AttractionFlexCard","Mixer_GeoVerticalNameWithButtonCard"],
                    "route":{
                        "page":"AttractionsFusion",
                        "params":{
                            "pagee":str(num_elemento_inicial),
                            "geoId":self.ciudad_id,
                            "contentType":"attraction",
                            "webVariant":"AttractionsFusion"
                        }
                    },
                    "mapSurface":False,
                    "debug":False,
                    "polling":False
                },
                "extensions":{"preRegisteredQueryId":"42974f26ab4c21f7"}
            }
        ]
        return payload
    
    
    # ------------------------------------------------------------------------ #
    # Otras funciones
    def __comprobar_y_crear_ruta_raiz(self):
        ruta_raiz = f'./data/scraping/pois/{self.ciudad_nombre}/'
        if not os.path.exists(ruta_raiz):
            pathlib.Path(ruta_raiz).mkdir(parents=True, exist_ok=True)
        self.__ruta_raiz = ruta_raiz


    def __comprobar_atributo_listado_pois(self, flag_redescarga=False):
        # Si no se tiene el atributo self.listado_pois se lee, y si tampoco lo tuviese se descarga
        if not hasattr(self, 'listado_pois'):
            ruta_lista_pois = self.__ruta_raiz + 'listado_pois.parquet'
            if os.path.exists(ruta_lista_pois) and not flag_redescarga:
                self.listado_pois = pd.read_parquet(ruta_lista_pois)
            else:
                self.obtener_listado_pois()

    
    # ------------------------ Funciones de descarga ------------------------- #
    def __descargar_informacion_poi(
            ciudad_id:str, ciudad_nombre:str, fila:pd.Series) -> None:
        # Descargar reviews del poi
        # Se obtienen los valores necesarios para construir el objeto Poi del
        # lisdo de pois
        poi_id = int(fila['Id']) # porque es un np.int64
        poi_nombre = fila['nombre']
        poi_ref = fila['ref']
        num_aprox_reviews = int(fila['cantidad reviews'])

        PoiSeleccionado = Poi(
            ciudad_id, ciudad_nombre, poi_id, poi_nombre, poi_ref, num_aprox_reviews)
        try:
            PoiSeleccionado.obtener_reviews()
        except Exception as e:
            print(f'Error en el poi {ciudad_nombre}: {e}')

    
    def __comprobar_descarga_reviews_poi(
            ciudad_id:str, ciudad_nombre:str, fila:pd.Series) -> None:
        # Descargar reviews del poi
        # Se obtienen los valores necesarios para construir el objeto Poi del
        # lisdo de pois
        poi_id = int(fila['Id']) # porque es un np.int64
        poi_nombre = fila['nombre']
        poi_ref = fila['ref']
        num_aprox_reviews = int(fila['cantidad reviews'])

        PoiSeleccionado = Poi(
            ciudad_id, ciudad_nombre, poi_id, poi_nombre, poi_ref, num_aprox_reviews)
        intentos = 2
        flag_parada = False
        while intentos > 0 and not flag_parada: 
            langs = PoiSeleccionado.se_obtuvieron_todas_las_reviews_por_lang()
            cantidad_nuevas_reviews = PoiSeleccionado.se_descargo_correctamente(
                flag_continuar_descarga=True)
            # Si se hubiese descargado completamente, la comprobación daría -1 
            # en cantidad de nuevas reviews
            # Si da 0, es que hay reviews que descargar pero no ha sido capaz de 
            # sacarlas, con lo que se debería proceder a comprobar las páginas 
            # mal descargadas
            if cantidad_nuevas_reviews == 0: 
                PoiSeleccionado.comprobar_reviews_paginas_descargadas()
            
            flag_parada = cantidad_nuevas_reviews == -1
            intentos -= 1


    def __descargar_imagenes_poi(
            ciudad_id:str, ciudad_nombre:str, fila:pd.Series) -> None:
        # Descargar reviews del poi
        poi_id = int(fila['Id']) # porque es un np.int64
        poi_nombre = fila['nombre']
        poi_ref = fila['ref']
        num_aprox_reviews = int(fila['cantidad reviews'])

        PoiSeleccionado = Poi(
            ciudad_id, ciudad_nombre, poi_id, poi_nombre, poi_ref, num_aprox_reviews)
        PoiSeleccionado.obtener_imagenes()


if __name__ == '__main__':
    ciudad_id = 187791
    ciudad_nombre = 'Rome_Lezio'
    ScraperPois = PoisTripadvisor(ciudad_id=ciudad_id, ciudad_nombre=ciudad_nombre)
    ScraperPois.obtener_reviews_listado_poi()


"""
GUÍA DE LAS FUNCIONES MÁS IMPORTANTES-------------------------------------------
*   ciudad.obtener_listado_pois(): Descarga el listado de pois de una ciudad
*   ciudad.obtener_reviews_listado_poi(): A través de multithreating, obtiene 
        las reviews de todos los pois del listado utilizando la clase Poi.
*   ciudad.obtener_listado_pois_de_pagina(pagina:int): Obtiene un df con los pois
        y sus características de la página en cuestión.
"""

"""
COLUMNAS DEL DF DE LISTADO DE POIS ---------------------------------------------
*   numero (de poi en la ciudad)
*   nombre (de poi)
*   puntuacion
*   cantidad reviews
*   ref
*   tags
*   Id (de poi)
"""

"""
EJEMPLO DE CONSTRUCTOR ---------------------------------------------------------
ScraperPois = PoisTripadvisor(ciudad_id=187791, ciudad_nombre=nombre_geo)
De momento no saca estos dos parámetros de forma automática
"""
