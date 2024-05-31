from src.pipeline.pipeline import Pipeline
# nohup python3 -u -m src.pipeline.prueba > 'data/pipeline/traza.log' &
# nohup '../miniconda3/envs/tf/bin/python3' -u -m src.pipeline.prueba > 'data/pipeline/traza.log' &

def main():
    # if len(sys.argv) > 1:
    #     cities2scrap_path = sys.argv[1]
    ciudad_a = 'Madrid'
    ciudad_b = 'Rome_Lazio'
    print('Se inicia la transformación de los archivos')

    pipeline = Pipeline(ciudad_a, ciudad_b)
    # pipeline.obtener_usuarios_interseccionados(flag_reintento_guardado=True)
    pipeline.obtener_codificacion_absoluta_multiproceso(flag_retomar=False)
    ruta_imagenes = './data/scraping/restaurantes/Madrid/imagenes/'
    pipeline.eliminar_elementos_erroneos(ruta_imagenes)

if __name__ == '__main__':
    main()