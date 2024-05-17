import pandas as pd
import numpy as np
import random
import os
# os.environ["CUDA_VISIBLE_DEVICES"] = "1"

import tensorflow as tf
from matplotlib import pyplot as plt
import seaborn as sns
import cv2
from typing import Literal


class PipelineTFData:
    """Pipeline para las transformaciones necesaria para convertir el conjunto
    en un tf.Data. 
    Requerimientos: 
        * df con la codificación de los items (y)
        * df con todas las reviews por item y usuario 
        (all_reviews_without_imgs.parquet)
    Para ello, se genera un df con multiindex (item, usr)"""
    def __init__(self, city_a:str, city_b:str, category_a:str, category_b:str, 
                 y_type:Literal['multiregresion', 'multilabel']) -> None:
        self.city_a = city_a
        self.city_b = city_b
        self.category_a = category_a
        self.category_b = category_b
        self.y_type = y_type
        # Tras ejecutar fit:
        # self.__images_root_dir_path
        # self.df_data # Almacena un df con multiindex(item, usr) -> y encoding
        # Tras ejecutar create_dataset
        # self.dataset

    # ------------------------------------------------------------------------ #
    # Funciones de fit
    @staticmethod
    def obtener_imagen_aleatoria(primitive_path, img_dir):
        img = random.choice(os.listdir(primitive_path + img_dir))
        img = img_dir + '/' + img
        return img
    @staticmethod
    def choose_image_randomly_by_directory(primitive_path, img_dir):
        img = random.choice(os.listdir(primitive_path + img_dir))
        img = img_dir + '/' + img
        return img
    

    @staticmethod
    def get_total_usrs_from_item(city:str, category:str) -> pd.DataFrame:
        items_and_usrs_path = f'./data/scraped data/{category} data/{city}/'
        items_and_usrs_path += 'all_reviews_without_imgs.parquet'
        df_items_and_usrs = pd.read_parquet(items_and_usrs_path)
        df_items_and_usrs = df_items_and_usrs.groupby('item')['user href'].count()
        return df_items_and_usrs


    @staticmethod
    def shuffle_and_split_data(df_data, val_proportion=.1):
        df_data = df_data.sample(frac=1, random_state=1)
        val_size = int(len(df_data)*val_proportion)
        train_size = len(df_data) - val_size
        df_train_data = df_data.iloc[:train_size]
        df_val_data = df_data.iloc[train_size:]
        return df_train_data, df_val_data


    def __get_cod_file(l=600) -> pd.DataFrame:
        l = str(l)
        cod_file = 'pois_Madrid_Athens_600_comments_filtered.xlsx'
        path = './data/codifications/' + cod_file
        filter = pd.read_excel(path, usecols=['Items', 'Incluir'])
        filter = filter.loc[filter['Incluir'].astype(bool), 'Items'].values
        path = './data/codifications/Madrid_Athens_multiregresion_2.parquet'
        df_cod = pd.read_parquet(path)
        df = df_cod.loc[:, filter]
        return df


    def __normalize_as_proportions(self, axis=1):
        # cod_file = f'{self.city_a}_{self.city_b}_{self.y_type}'
        # codificationes_path = f'./data/codifications/{cod_file}.parquet'
        # df_cod = pd.read_parquet(codificationes_path)
        df_cod = self.__get_cod_file()
        max_val_arr = df_cod.apply(lambda arr: arr.max(), axis=axis).values
        max_val_arr[max_val_arr == 0] = 1
        max_val_arr = np.repeat(max_val_arr, df_cod.shape[1]).reshape(df_cod.shape)
        # df = df / max_val_arr[:, None] # Usando broadcasting, como se puede en df se 
        df_cod_norm = df_cod / max_val_arr
        return df_cod_norm, df_cod


    def extend_items_with_usrs(self, df_with_idx_items:pd.DataFrame) -> pd.DataFrame:
        primitive_path = f'./data/scraped data/{self.category_a} data/{self.city_a}/'
        items_and_usrs_path = primitive_path + 'all_reviews_without_imgs.parquet'
        images_root_dir_path = primitive_path + 'available images/'

        df_items_and_usrs = pd.read_parquet(items_and_usrs_path)
        df_items_and_usrs.set_index(['item'], inplace=True)
        df_extended = df_with_idx_items.join(df_items_and_usrs, how='inner')
        df_extended.rename({'user href': 'usr'}, axis=1, inplace=True)
        df_extended = df_extended[df_extended['usr'].notna()]
        df_extended['usr'] = df_extended['usr'].map(lambda val: val.split('/')[-1])
        df_extended.reset_index(inplace=True)
        # los items pasan de ser solo los directorios de los items a item/imagen 
        # aleatoria
        df_extended['item'] = df_extended['item'].map(
            lambda item: PipelineTFData.choose_image_randomly_by_directory(
                images_root_dir_path, item))
        df_extended.set_index(['item', 'usr'], inplace=True)

        # Formato: index -> (item, usr); valores -> vectores de codificación (y)
        return df_extended
    # ------------------------------------------------------------------------ #

    # ------------------------------------------------------------------------ #
    # Funciones de data_generator
    def __load_and_preprocess_image(self, image_path, label):
        # Función que carga la imagen usando ImageDataGenerator
        # Se reescala la imagen durante la carga
        img = tf.keras.preprocessing.image.load_img(
            self.__images_root_dir_path + image_path.decode("utf-8"), 
            target_size=(299, 299))  
        img_array = tf.keras.preprocessing.image.img_to_array(img)
        img_array = (img_array / 127.5) - 1.0
        label = np.ndarray.astype(label, np.float64)
        img_array = np.ndarray.astype(img_array, np.float64)
        return label, img_array
    # ------------------------------------------------------------------------ #

    # ------------------------------------------------------------------------ #
    # Funciones principales
    def fit(self, shuffle_and_split_restaurants=False, val_prop=0.1): 
        # se puede escoger que se haga un split a nivel de restaurante
        df_y, df_y_not_normalized = self.__normalize_as_proportions()
        self.__images_root_dir_path = f'./data/scraped data/{self.category_a} data/{self.city_a}/available images/'
        
        if shuffle_and_split_restaurants:
            df_train_y, df_val_y = PipelineTFData.shuffle_and_split_data(
                df_y, val_proportion=val_prop)
            self.df_data_train = self.extend_items_with_usrs(df_train_y)
            self.df_data_val = self.extend_items_with_usrs(df_val_y)

            train_idx = df_train_y.index
            val_idx = df_val_y.index

            self.df_data_train_not_normalized = self.extend_items_with_usrs(
                df_y_not_normalized.loc[train_idx])
            self.df_data_val_not_normalized = self.extend_items_with_usrs(
                df_y_not_normalized.loc[val_idx])

        else:
            self.df_data = self.extend_items_with_usrs(df_y)
            self.df_data_not_normalized = self.extend_items_with_usrs(df_y_not_normalized)


    # Creación del dataset con tf.Data
    def create_dataset(self, val_proportion=.1, batch_size=512):
        image_paths = self.df_data.index.get_level_values(0)
        # Se coge las columnas de las y's
        labels = self.df_data.values

        # Se crea un tf.data.Dataset con las listas de directorios y los vectores y
        dataset = tf.data.Dataset.from_tensor_slices((image_paths, labels))

        # Se cargan y preprocesan las imágenes usando ImageDataGenerator en paralelo 
        # con un map
        dataset = dataset.map(
            lambda image_path, label: tf.numpy_function(
                self.__load_and_preprocess_image,
                [image_path, label],
                [tf.float64, tf.float64]
            ),
            num_parallel_calls=tf.data.experimental.AUTOTUNE
        )

        # Shuffle, batch, and prefetch for better performance
        # El prefetch es la precarga de batches en memoria
        dataset = dataset.shuffle(buffer_size=len(self.df_data))
        dataset = dataset.batch(batch_size)
        dataset = dataset.prefetch(buffer_size=tf.data.experimental.AUTOTUNE)

        return dataset
    # ------------------------------------------------------------------------ #


# Pruebas del generador de tf.data fuera de la clase
def load_and_preprocess_image(image_path):
    # Función que carga la imagen usando ImageDataGenerator
    # Se reescala la imagen durante la carga
    image_path = image_path.decode('utf-8')
    path = './data/scraped data/restaurants data/Madrid/available images/' + image_path
    img = tf.keras.preprocessing.image.load_img(path, target_size=(299, 299))  
    img_array = tf.keras.preprocessing.image.img_to_array(img)
    img_array = (img_array / 127.5) - 1.0
    # label = np.ndarray.astype(label, np.float64)
    img_array = np.ndarray.astype(img_array, np.float64)
    return img_array


def shuffle_and_split_data(df_data, val_proportion=.1):
    df_data = df_data.sample(frac=1)
    val_size = int(len(df_data)*val_proportion)
    train_size = len(df_data) - val_size
    df_train_data = df_data.iloc[:train_size]
    df_val_data = df_data.iloc[train_size:]
    return df_train_data, df_val_data


def create_dataset(df_data:pd.DataFrame, batch_size=512):
    df_data.to_parquet('./data/codifications2/codificacion_normalizada.parquet')
    image_paths = df_data.index.get_level_values(0)
    # Se coge las columnas de las y's
    labels = df_data.values

    # Se crea un tf.data.Dataset con las listas de directorios y los vectores y
    dataset_y = tf.data.Dataset.from_tensor_slices(labels)
    dataset_x = tf.data.Dataset.from_tensor_slices(image_paths)
    # Se cargan y preprocesan las imágenes usando ImageDataGenerator en paralelo 
    # con un map
    # dataset = dataset.shuffle(buffer_size=len(df_data))
    dataset_x = dataset_x.map(
        lambda image_path: tf.numpy_function(
            load_and_preprocess_image,
            [image_path],
            [tf.float64]
            ),
        num_parallel_calls=tf.data.experimental.AUTOTUNE
    )
    dataset = tf.data.Dataset.zip((dataset_x, dataset_y))
    # Shuffle, batch, and prefetch for better performance
    # El prefetch es la precarga de batches en memoria
    #dataset = dataset.shuffle(buffer_size=len(df_data))
    dataset = dataset.batch(batch_size)
    dataset = dataset.prefetch(buffer_size=tf.data.experimental.AUTOTUNE)

    return dataset


# Ejemplo
if __name__ == '__main__':
    # Las mismas funciones 
    
    city = 'Madrid'
    category = 'restaurants'

    # Generador
    # datagen = tf.keras.preprocessing.image.ImageDataGenerator(rescale=1./127.5)

    # Pruebas
    city_a = 'Madrid'
    category_a = 'restaurants'
    city_b = 'Athens'
    category_b = 'pois'

    pipeline = PipelineTFData(
        city_a, city_b, category_a, category_b, 'multiregresion')
    pipeline.fit()

    df_data = pipeline.df_data_train
    df_train, df_val = shuffle_and_split_data(df_data)
    val_dataset = create_dataset(df_data, batch_size=32)
    train_dataset = create_dataset(df_data, batch_size=64)
    # dataset = pipeline.create_dataset(batch_size=512)

    for element in train_dataset:
        print('Control1')
        print(element)
        print('Control2')
        exit

"""
    # Prueba con load_and_process_image
    image_path = pipeline.df_data.index.get_level_values(0)[0]
    label = pipeline.df_data.values[0]
    print(image_path)
    print(label)
    datos = load_and_preprocess_image(image_path, label)
    print(datos[0])
    print(datos[1])

    dataset = create_dataset(pipeline.df_data, batch_size=512)
    # dataset = pipeline.create_dataset(batch_size=512)

    for element in dataset:
        print(element)
        print(element.shape)
        exit
"""