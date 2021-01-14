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

##############################################################################
## PARAMETERS DEFINITION
##############################################################################
#Rango de fechas para extraer la curva de carga de los CUPS. AMBAS INCLUÍDAS
#FORMATO: AÑO, MES, DIA.        AÑO CON 4 CIFRAS, MES Y DIA SIN COMPLETAR CON 0 DELANTE.
#Fecha de inicio:
fecha_ini=datetime.datetime(2020, 1, 1)
#Fecha de fin:
fecha_fin=datetime.datetime(2020, 1, 1)

#Información del CT sobre el que se aplicará el cálculo de pérdidas
Nombre_CT='MIRAMONTE'

##############################################################################
## FILES READING
##############################################################################
ruta_raiz = os.path.dirname(os.path.abspath(__file__)) + '\\' #Directorio donde está el .py, las librerías y los archivos
#Archivos de topología, trazas y CUPS del CT
archivo_topologia = ruta_raiz + r'Fichero_TOPOLOGIA_DEPERTEC.csv'
archivo_traza = ruta_raiz + r'Fichero_TRAZA_DEPERTEC.csv'
archivo_ct_cups = ruta_raiz + r'Fichero_CT_CUPS_DEPERTEC.csv'

#Nombre del archivo de trazas generado con las longitudes calculadas
archivo_traza_mod = ruta_raiz + r'Fichero_TRAZA_DEPERTEC_mod.csv'

#Ruta donde se encuentran todos los archivos de curvas de carga.
ruta_cch = 'F:/GTEA/DEPERTEC/20201120_Ficheros_CCH_Miramonte_y_Xustas/'

#Archivo de configuración con la conexión SQL
archivo_config = ruta_raiz + r"Config.txt"
#Ruta y nombre del archivo de logout.
ruta_log = ruta_raiz + 'Log_' + Nombre_CT + '_DEPERTEC.log'



##############################################################################
## OPTIONAL PARAMETERS
##############################################################################
#Llamar a la función con todos los parámetros admisibles:
V_Linea = 400.0
X_cable = 0
temp_cables = 20
save_plt_graph = 0
# 0 = Guardar dos imágenes que representan el grafo.
# 1 = No se guardan las imágenes


## SQL INFORMATION
#Estas tablas deben estar creadas ya en el SQL. El nombre de las primeras se genera automáricamente dentro de la librería. 
#tabla_ct_nodos = "OUTPUT_8417_MIRAMONTE_NODOS"
#tabla_ct_trazas = "OUTPUT_8417_MIRAMONTE_TRAZAS"
#tabla_ct_nodos = "OUTPUT_1462_XUSTAS_NODOS"
#tabla_ct_trazas = "OUTPUT_1462_XUSTAS_TRAZAS"
tabla_cts_general = "OUTPUT_PERDIDAS_AGREGADOS_CT"
guardado_bbdd = 0
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


   
    
fecha_datetime = fecha_ini
while fecha_datetime < fecha_fin + datetime.timedelta(days=1):
#    print(fecha_datetime)
    ##Llamar a la función con todos los parámetros admitidos: 
    ga.Solve_Graph(fecha_datetime, Nombre_CT, ruta_raiz, archivo_topologia, archivo_traza, archivo_traza_mod, archivo_ct_cups, ruta_cch, archivo_config, ruta_log, V_Linea, X_cable, temp_cables, save_plt_graph, guardado_bbdd, tabla_cts_general, log_mode)
    
    ##Llamar a la función con únicamente con los parámetros básicos: 
#    ga.Solve_Graph(fecha_datetime, Nombre_CT, ruta_raiz, archivo_topologia, archivo_traza, archivo_traza_mod, archivo_ct_cups, ruta_cch, archivo_config, ruta_log)

    fecha_datetime = fecha_datetime + datetime.timedelta(days=1)












