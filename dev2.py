import os
import cv2
import shutil
import pandas as pd
import numpy as np

path = "./data/scraping/restaurantes/Madrid/imagenes/Restaurante AKI T'Spero/12321104_media_photo-m_1280_15_cf_62_a5_no-es-como-el-del-rey.jpg"

def comprobar_imagen_vacia(ruta_imagen:str) -> bool:
    imagen = cv2.imread(ruta_imagen)
    return imagen is None


def seleccionar_jpgs(ruta_imagenes_elemento:str) -> bool:
    rutas_imagenes = [
        f 
        for f in os.listdir(ruta_imagenes_elemento) 
        if f.endswith('.jpg')]
    return rutas_imagenes


def eliminar_imagenes_vacias_del_elemento(ruta_imagenes_elemento:str) -> None:
    for imagen in seleccionar_jpgs(ruta_imagenes_elemento):
        ruta_imagen = ruta_imagenes_elemento + imagen
        if comprobar_imagen_vacia(ruta_imagen):
            os.remove(ruta_imagen)
    f_restantes = os.listdir(ruta_imagenes_elemento)
    if ( len(f_restantes) == 1 and f_restantes[0].endswith('.parquet') ) or ( len(f_restantes) == 0 ):
        print(f'Directorio {ruta_imagenes_elemento} eliminado por no tener imágenes válidas')
        shutil.rmtree(ruta_imagenes_elemento)


def eliminar_elementos(ruta_imagenes:str) -> None:
    for ruta_elemento in os.listdir(ruta_imagenes):
        if ruta_elemento != '.DS_Store':
            eliminar_imagenes_vacias_del_elemento(ruta_imagenes + ruta_elemento + '/')



if __name__ == '__main__':
    # eliminar_elementos('./data/scraping/restaurantes/Madrid/imagenes/') implementar como paso final de la pipeline
    pois = seleccionar_pois_por_popularidad_y_etiquetas('Rome_Lazio', 1000)

    print(pois)