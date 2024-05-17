from src.pipeline.codificacion import Codificador
# nohup '../miniconda3/envs/tf/bin/python3' -u -m src.pipeline.main_codificacion dev > 'data/pipeline/pivot.log' &

def main():
    # if len(sys.argv) > 1:
    #     cities2scrap_path = sys.argv[1]
    ciudad_a = 'Madrid'
    ciudad_b = 'Rome_Lazio'
    print('Se inicia la transformación de los archivos')

    codificador = Codificador(ciudad_a, ciudad_b)
    codificador.generar_df_codificacion_a_partir_del_cod_tmp(eliminar_f_intersecciones_tmp=False)


if __name__ == '__main__':
    main()