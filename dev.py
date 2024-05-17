import pandas as pd
# nohup '../miniconda3/envs/tf/bin/python3' -u -m dev > 'data/pipeline/pivot.log' &

ruta = './data/pipeline/Madrid_restaurantes__Rome_Lazio_pois/'

ruta_pivot = ruta + 'df_codificacion.parquet'

print(pd.read_parquet(ruta_pivot).shape)

