from src.scraper.ciudad_pois import PoisTripadvisor


if __name__ == '__main__':
    ciudad_id = 186338
    ciudad_nombre = 'London'

    londres = PoisTripadvisor(ciudad_id, ciudad_nombre)
    londres.obtener_listado_pois()