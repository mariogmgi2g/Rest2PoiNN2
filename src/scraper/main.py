from src.scraper.ciudad_pois import PoisTripadvisor
from src.scraper.ciudad_restaurantes import RestaurantesTripadvisor
from src.scraper.poi import Poi
from src.scraper.restaurante import Restaurante
import os
import sys
import json


def main():
    """
    arg1 - tipo (pois, restaurantes)
    arg2 - nombre ciudad
    arg3 - id ciudad
    """
    # if len(sys.argv) > 1:
    #     cities2scrap_path = sys.argv[1]
    ciudad_id = 186338
    ciudad_nombre = 'London'
    ScraperPois = PoisTripadvisor(ciudad_id=ciudad_id, ciudad_nombre=ciudad_nombre)
    if not os.path.exists(f'./data/scraping/pois/{ciudad_nombre}/reviews'):
        ScraperPois.descargar_informacion_pois()
    ScraperPois.retomar_descarga_informacion_pois()

    # ciudad_id = 187514
    # ciudad_nombre = 'Madrid'
    # ScraperRestaurantes = RestaurantesTripadvisor(ciudad_id=ciudad_id, ciudad_nombre=ciudad_nombre)
    # # ScraperRestaurantes.descargar_informacion_restaurantes() # INCLUYE LAS IMÁGENES
    # ScraperRestaurantes.retomar_descarga_informacion_restaurantes() # INCLUYE LAS IMÁGENES
    # ScraperRestaurantes.descargar_imagenes_restaurantes()
    """Ejemplo POI
    nombre = 'Arco_di_Tito'
    ciudad_id = 187791
    ciudad_nombre = 'Rome_Lazio'
    poi_id = 245880
    num_aprox_reviews = 100
    ref = '/Attraction_Review-g187791-d245880-Reviews-Arco_di_Tito-Rome_Lazio.html'

    poi = Poi(ciudad_id, ciudad_nombre, poi_id, nombre, ref, num_aprox_reviews)
    poi.se_descargo_correctamente(flag_continuar_descarga=True)
    """

if __name__ == '__main__':
    main()



    