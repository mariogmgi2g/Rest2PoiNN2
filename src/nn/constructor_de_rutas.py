import os 
import pathlib


class ConstructorDeRutas:
    """Clase con la información de los directorios y rutas para no tener que 
    repetirlo constantemente"""
    
    def __init__(
            self, ciudad_a:str, ciudad_b:str, 
            tipo_a:str='restaurantes', tipo_b:str='pois') -> None:
        
        self.__ciudad_a = ciudad_a
        self.__tipo_a = tipo_a

        self.__ciudad_b = ciudad_b
        self.__tipo_b = tipo_b

    
    # Rutas --------------------------------------------------------------------
    def obtener_ruta_pipeline(self) -> str:
        ruta_pipeline = f'./data/pipeline/{self.ciudad_a}_{self.tipo_a}__{self.ciudad_b}_{self.tipo_b}/'
        if not os.path.exists(ruta_pipeline):
            pathlib.Path(ruta_pipeline).mkdir(parents=True, exist_ok=True)
        return ruta_pipeline


    def obtener_ruta_nn(self) -> str:
        ruta_nn = f'./data/nn/{self.ciudad_a}_{self.tipo_a}__{self.ciudad_b}_{self.tipo_b}/'
        if not os.path.exists(ruta_nn):
            pathlib.Path(ruta_nn).mkdir(parents=True, exist_ok=True)
        return ruta_nn
    

    def obtener_ruta_scraping(self, de_ciudad_a:bool) -> str:
        if de_ciudad_a: 
            ciudad = self.ciudad_a
            tipo = self.tipo_a
        else: # Si no es de la ciudad b
            ciudad = self.ciudad_b
            tipo = self.tipo_b

        ruta_scraping = f'./data//scraping/{tipo}/{ciudad}/'
        return ruta_scraping
    

    def obtener_ruta_imagenes(self, de_ciudad_a:bool) -> str:
        ruta_scraping = self.obtener_ruta_scraping(de_ciudad_a)
        ruta_imagenes = ruta_scraping + 'imagenes/'
        return ruta_imagenes
    

    def obtener_ruta_reviews(self, de_ciudad_a:bool) -> str:
        ruta_scraping = self.obtener_ruta_scraping(de_ciudad_a)
        ruta_reviews = ruta_scraping + 'reviews/'
        return ruta_reviews


    # Getters ------------------------------------------------------------------
    @property
    def ciudad_a(self):
        return self.__ciudad_a
    
    @property
    def tipo_a(self):
        return self.__tipo_a
    
    @property
    def ciudad_b(self):
        return self.__ciudad_b
    
    @property
    def tipo_b(self):
        return self.__tipo_b
    
