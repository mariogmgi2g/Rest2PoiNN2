import os
from src.nn.constructor_de_rutas import ConstructorDeRutas


if __name__ == '__main__':
    ciudad_a = 'Madrid'
    tipo_a = 'restaurantes'
    ciudad_b = 'Rome_Lazio'
    tipo_b = 'pois'

    constructor_rutas = ConstructorDeRutas(
        ciudad_a, ciudad_b, tipo_a=tipo_a, tipo_b=tipo_b)
    
    existe_imagenes_a = os.path.exists(constructor_rutas.obtener_ruta_imagenes(de_ciudad_a=True))
    print(f'¿Existe ruta de imágenes de {constructor_rutas.ciudad_a}?: {existe_imagenes_a}')

    ruta_reviews = constructor_rutas.obtener_ruta_reviews(de_ciudad_a=True)
    existe_reviews_a = os.path.exists(ruta_reviews)
    print(f'¿Existe ruta de imágenes de {constructor_rutas.ciudad_a} ({constructor_rutas.tipo_a})?: {existe_reviews_a}')
    print(ruta_reviews)

    existe_ruta_nn = os.path.exists(constructor_rutas.obtener_ruta_nn())
    print(f'¿Existe ruta de nn?: {existe_ruta_nn}')