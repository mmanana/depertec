# -*- coding: utf-8 -*-

"""
    File name: graph_losses_calculation.py
    Author: Mario Mañana, David Carriles, GTEA
    Date created: 6 Aug 2020
    Date last modified: 12 Jan 2021
    Python Version: 3.6
    
    Define parameters and graphanalysis.py library call.
    Calculate and save losses for two dates interval period.
    
    Required cable.py and graphanalysis.py functions in the same folder.
"""

import datetime
import os

import graphanalysis as ga #Llamar al archivo graphanalisys.py
import codecs

##############################################################################
## PARAMETERS DEFINITION
##############################################################################
#Rango de fechas para extraer la curva de carga de los CUPS. AMBAS INCLUÍDAS
#FORMATO: AÑO, MES, DIA.        AÑO CON 4 CIFRAS, MES Y DIA SIN COMPLETAR CON 0 DELANTE.
#Fecha de inicio:
fecha_ini=datetime.datetime(2020, 1, 1)
#Fecha de fin:
fecha_fin=datetime.datetime(2020, 1, 31)


##############################################################################
## FILES READING
##############################################################################
ruta_raiz = os.path.dirname(os.path.abspath(__file__)) + '\\'         #Directorio donde está el .py, las librerías y los archivos
#Archivos de topología, trazas y CUPS del CT
archivo_topologia = ruta_raiz + r'Fichero_TOPOLOGIA_DEPERTEC.csv'
archivo_traza = ruta_raiz + r'Fichero_TRAZA_DEPERTEC.csv'
archivo_ct_cups = ruta_raiz + r'Fichero_CT_CUPS_DEPERTEC.csv'

ruta_raiz2='F:\GTEA\DEPERTEC\Ficheros_Piloto_de_Validacion\\'
archivo_topologia = ruta_raiz2 + r'20210204_TOPOLOGIA.csv'#Fichero_TOPOLOGIA_DEPERTEC.csv'
archivo_traza = ruta_raiz2 + r'20210204_TRAZA.csv'#Fichero_TRAZA_DEPERTEC.csv'
archivo_ct_cups = ruta_raiz2 + r'20210204_CT_CUPS.csv'#Fichero_CT_CUPS_DEPERTEC.csv'


#Ruta donde se encuentran todos los archivos de curvas de carga.
ruta_cch = 'F:/GTEA/DEPERTEC/20201120_Ficheros_CCH_Miramonte_y_Xustas/'
ruta_cch = 'F:/GTEA/DEPERTEC/Ficheros_Piloto_de_Validacion/CCH/'


#Archivo de configuración con la conexión SQL
archivo_config = ruta_raiz + r"Config.txt"

#Archivo de configuración con el Nombre y los IDs de todos los CTs a analizar
archivo_CTs = ruta_raiz + r'CT_analysis.txt'


##############################################################################
## OPTIONAL PARAMETERS
##############################################################################
#Llamar a la función con todos los parámetros admisibles:
V_Linea_400 = 400.0
V_Linea_230 = 230.0
X_cable = 0
temp_cables = 20

use_gml_file = 0
# 0 = Usar un archivo .gml o .gml.gz de la carpeta /gml_files que contiene una descripción del grafo ya generado.
# 1 = No se usa un archivo .gml o .gml.gz, se genera el grafo desde cero con los .csv (por defecto).

save_csv_mod = 1
# 0 = Guardar en la carpeta /csv los archivos .csv modificados con la información del grafo: Nodos, Trazas, CT_CUPS, Matriz_Distancias.
# 1 = No se guardan los archivos (por defecto).

save_plt_graph = 0
# 0 = Guardar dos imágenes que representan el grafo.
# 1 = No se guardan las imágenes (por defecto).

#Parámetros para estimar el error cometido al calcular las pérdidas.
#Si el valor de curvas de carga + pérdidas está ligeramente por debajo (limite_inferior) o ligeramente por encima (limite_superior) del valor medido en el CT se considera aceptable.
upper_limit = 10 #% permitido para considerar un valor aceptable de cch+pérdidas, por encima del valor medido en el CT
lower_limit = 10 #% permitido para considerar un valor aceptable de cch+pérdidas, por debajo delvalor medido en el CT


## SQL INFORMATION
tabla_cts_general = "OUTPUT_PERDIDAS_AGREGADOS_CT"
save_ddbb = 1
# 0 = Guardar todos los resultados en todas las tablas.
# 1 = Guardar solo en la tabla de agregados CT.
# 2 = Guardar solo en las tablas generales de cada CT con todos los datos del grafo, sin agregados CT.
# 3 = NO guardar nada.

#Logging mode
log_mode = 'logging.DEBUG'
# logging.INFO
# logging.WARNING
# logging.ERROR
# logging.CRITICAL


#Se lee el archivo que contiene el nombre e ID de todos los CTs a analizar
cont_errores = 0
with codecs.open(archivo_CTs, 'r', encoding='utf-8') as reader:
    while True:
        try:
            Nombre_CT = str(reader.readline().splitlines()[0])
            if Nombre_CT == 'FIN':
                break
            id_ct = int(reader.readline().splitlines()[0])
            
            #if len(Nombre_CT) > 1:
            if Nombre_CT != 'FIN':
                #Ruta y nombre del archivo de logout.
                ruta_log = ruta_raiz + 'log_files/Log_' + Nombre_CT.replace(' ', '_') + '_' + str(id_ct) + '_DEPERTEC.log'
                ga.Solve_Graph(fecha_ini=fecha_ini, fecha_fin=fecha_fin, Nombre_CT=Nombre_CT, id_ct=id_ct, archivo_topologia=archivo_topologia, archivo_traza=archivo_traza, archivo_ct_cups=archivo_ct_cups, ruta_cch=ruta_cch, archivo_config=archivo_config, ruta_log=ruta_log, V_Linea_400=V_Linea_400, V_Linea_230 = V_Linea_230, X_cable=X_cable, temp_cables=temp_cables, use_gml_file=use_gml_file, save_csv_mod=save_csv_mod, save_plt_graph=save_plt_graph, save_ddbb=save_ddbb, tabla_cts_general=tabla_cts_general, log_mode=log_mode, upper_limit=upper_limit, lower_limit=lower_limit)

        except:
            pass
            # break
        
        


    





