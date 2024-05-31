import numpy as np
import pandas as pd
from src.nn.constructor_de_rutas import ConstructorDeRutas
from src.nn.filtro_generador import FiltroGenerador
from src.nn.baselines import Baselines
from src.nn.metricas import Metricas
import math
import os
import random
from src.nn.generador2 import Generador
import tensorflow as tf
import logging
# import pdb


tf.config.run_functions_eagerly(True)
tf.data.experimental.enable_debug_mode()

# logging.basicConfig(level=logging.INFO, filename='tf_procesado_imgs.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)


if __name__ == '__main__':
    cols = ['col1', 'col2', 'col3']
    filas = ['fila1', 'fila2', 'fila3', 'fila4']

    df1 = pd.DataFrame(np.random.uniform(0, 10, (4, 3)))
    df1.index = filas
    df1.columns = cols

    df2 = pd.DataFrame(np.random.uniform(0, 10, (4, 3)))
    df2.index = filas
    df2.columns = cols

    print( Metricas.obtener_metricas_rankings(df1, df2, top=4) )