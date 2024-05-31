
import os
os.environ['TF_GPU_ALLOCATOR'] = 'cuda_malloc_async'

import tensorflow as tf
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import typing


from src.nn.constructor_de_rutas import ConstructorDeRutas
from src.nn.metricas import Metricas

BATCH_SIZE = 264

class NN:
    def __init__(
            self, rutas:ConstructorDeRutas, 
            datasets:typing.Tuple[tf.data.Dataset], n_vector_cod:int) -> None:
        if len(datasets) != 3:
            raise ValueError(
                'El parámetro "datasets" tiene que ser una tupla de tres elementos,'
                ' siendo los de entrenamiento, validación y test. Pueden ser '
                'tf.data.Dataset o None')
        
        self.parametros_conjunto = rutas
        self.dataset_train, self.dataset_val, self.dataset_test = datasets
        self.__n_vector_cod = n_vector_cod
        self.__construir_modelo()

        self.__ruta_n = self.parametros_conjunto.obtener_ruta_nn() + 'nn1/'
        if not os.path.exists(self.__ruta_n):
            os.mkdir(self.__ruta_n)


    def __construir_modelo(self):
        modelo_base = tf.keras.applications.InceptionV3(
            include_top=False, 
            weights='imagenet',
            input_shape=(299, 299, 3)
        )
        # Se congelan las capas para que no se muevan sus pesos
        modelo_base.trainable = False

        # Se añaden capas para adaptarse al problema
        x = modelo_base.output
        x = tf.keras.layers.GlobalAveragePooling2D()(x)
        x = tf.keras.layers.BatchNormalization()(x)
        x = tf.keras.layers.Dense(self.__n_vector_cod, activation='relu')(x)
        
        salida = tf.keras.layers.Dense(self.__n_vector_cod, activation='sigmoid')(x)

        # Me aseguro de que las capas del modelo base estén congeladas
        for layer in modelo_base.layers:
            layer.trainable = False

        # Se ensambla el modelo
        modelo = tf.keras.Model(inputs=modelo_base.input, outputs=salida)
        print(modelo.summary)
        self.modelo = modelo


    def fit(self):
        # Se introducen los callbacks
        es = tf.keras.callbacks.EarlyStopping(
            monitor='val_loss', mode='min', patience=3, verbose=1)
        mc = tf.keras.callbacks.ModelCheckpoint(
            self.__ruta_n + 'mejor_modelo_nn1.h5', 
            monitor='val_loss', mode='min',  save_best_only=True, verbose=1)
        
        # Se compila el modlo
        self.modelo.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=1e-4), 
            loss=Metricas.rmse, metrics=[Metricas.rmse, 'mse', 'mae'])
        
        self.dataset_train

        # Se entrena el modelo
        history = self.modelo.fit(
            self.dataset_train, validation_data=self.dataset_val, epochs=5, 
            callbacks=[es, mc], verbose=1, batch_size=BATCH_SIZE)
        
        self.history = history
        self.__guardar_history_plot()

    
    def __guardar_history_plot(self):
        # Resumen de la loss
        plt.plot(self.history.history['loss'], label='train')
        plt.plot(self.history.history['val_loss'], label='test')
        plt.title('Loss evolution')
        plt.savefig(self.__ruta_n + 'Precision.jpg')

    
    @staticmethod  
    def cargar_modelo_entrenado(rutas:ConstructorDeRutas):
        ruta_modelo = rutas.obtener_ruta_nn() + 'nn1/mejor_modelo_nn1.h5'
        modelo = tf.keras.models.load_model(
            ruta_modelo, 
            custom_objects={'loss': Metricas.rmse, 'rmse': Metricas.rmse}
        )
        return modelo
    
    