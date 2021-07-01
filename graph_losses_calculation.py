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
fecha_fin=datetime.datetime(2020, 1, 2)

# Revisar. 2 lazos seguidos
# EL PUNTAL
# ID_CT: 3666


#Información del CT sobre el que se aplicará el cálculo de pérdidas
#Necesario el nombre y el identificador.
# Nombre_CT='MIRAMONTE'
# id_ct = 8417

# Nombre_CT = 'XUSTAS'
# id_ct = 1462

# Nombre_CT = 'TETUAN'
# id_ct = 10735

# Nombre_CT = 'CANALEJAS' #Bucle y nodos aislados
# id_ct = 14188

# Nombre_CT = 'S.CORAZONES'
# id_ct = 11843

# Nombre_CT = 'CALLEJA ARNA' #Bucle
# id_ct = 12703

# Nombre_CT = 'EL NOGAL'
# id_ct = 12891

# Nombre_CT = 'EL INFIERNO'
# id_ct = 14066

# Nombre_CT = 'NOGUES'
# id_ct = 14601

# Nombre_CT = 'PROGRESO'
# id_ct = 13878

# Nombre_CT = 'CRUZ BLANCA'
# id_ct = 12598

# Nombre_CT = 'AREA 74'
# id_ct = 14628

# Nombre_CT = 'EL CASTILLO'
# id_ct = 14016

# Nombre_CT = 'SAN LORENZO'
# id_ct = 14959

# Nombre_CT = 'C.T. 1 CAZOÑA'
# id_ct = 12560

# Nombre_CT = 'RCIA. ANCIANOS' #Bucles múltiples no resolubles
# id_ct = 14138

# Nombre_CT = 'AVDA. DEL DEPORTE'
# id_ct = 12016

# Nombre_CT = 'LA SARA'
# id_ct = 4381

# Nombre_CT = 'URB. LOS PUERTOS 2'
# id_ct = 1110

# Nombre_CT = 'URB. LOS PUERTOS 1'
# id_ct = 14345

# Nombre_CT = 'MARISMA DE ORIA'
# id_ct = 2077

# Nombre_CT = 'PUNTO LIMPIO'
# id_ct = 9343

# Nombre_CT = 'LA VENTILLA'
# id_ct = 9142

# Nombre_CT = 'EL PILAR' #Sin valores medidos en el CT
# id_ct = 6720

# Nombre_CT = 'LA LASTRA'
# id_ct = 5692

# Nombre_CT = 'C.T.6 GUARNIZO'
# id_ct = 13441

# Nombre_CT = 'C.T.3 MORERO'
# id_ct = 11707

# Nombre_CT = 'IMEGU'
# id_ct = 1843

# Nombre_CT = 'YESERIA'
# id_ct = 14715

# Nombre_CT = 'LOS PUERTOS GUARNIZO'
# id_ct = 10248

# Nombre_CT = 'LA CUEVA' #NO TIENE NINGUNA DESCRIPCIÓN DE NODOS O TRAZAS, SOLO CUPS.
# id_ct = 2515

# Nombre_CT = 'S.BAHIA MONEO'
# id_ct = 4969

# Nombre_CT = 'RUELO'
# id_ct = 12603

# Nombre_CT = 'MERCADO DE GANADOS'
# id_ct = 12782

# Nombre_CT = 'CARCEL'
# id_ct = 12434

# Nombre_CT = 'IGLESIA S. LORENZO'
# id_ct = 2221

# Nombre_CT = 'BARRUELO DE VILLADIEGO'
# id_ct = 10980

# Nombre_CT = 'LA FUENTE'
# id_ct = 8193

# Nombre_CT = 'ALTO LOS PALOMARES'
# id_ct = 14902

# Nombre_CT = 'MORO'
# id_ct = 4014

# Nombre_CT = 'TEJERA VIEJA'
# id_ct = 4604

# Nombre_CT = 'CASAS M.O.P.U.'
# id_ct = 2406

# Nombre_CT = 'LAGUNA DEL OLEO'
# id_ct = 11698

# Nombre_CT = 'ACACIAS 2'
# id_ct = 13897

# Nombre_CT = 'TIRSO'
# id_ct = 11851

# Nombre_CT = 'ANTHANA 2'
# id_ct = 13548

# Nombre_CT = 'SECTOR 4 CT 9'
# id_ct = 12837

# Nombre_CT = 'VERDEMAR'
# id_ct = 13149

# Nombre_CT = 'ALBERICO PARDO'
# id_ct = 11662

# Nombre_CT = 'PEÑA VERDE'
# id_ct = 13583

# Nombre_CT = 'IGLESIA PEÑACASTILLO' #No hay agregado en el ct para la fecha 26-1-2020
# id_ct = 12507

# Nombre_CT = 'EL BOSQUE'
# id_ct = 14548

# Nombre_CT = 'SOLARES'
# id_ct = 4663

# Nombre_CT = 'PUENTE SOCUEVA'
# id_ct = 4449

# Nombre_CT = 'FONTELA'
# id_ct = 13628

# Nombre_CT = 'PENELAS'
# id_ct = 86

# Nombre_CT = 'BARXA'
# id_ct = 526

# Nombre_CT = 'GRAÑA'
# id_ct = 832

# Nombre_CT = 'LA GARGANTA'
# id_ct = 6876

# Nombre_CT = 'ESPIÑO-DOCAL' #Autoconsumo
# id_ct = 1060


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
# #Ruta y nombre del archivo de logout.
# ruta_log = ruta_raiz + 'Log/Log_' + str(id_ct) + '_' + Nombre_CT.replace(' ', '_') + '_DEPERTEC.log'

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

use_gml_file = 1
# 0 = Usar un archivo .gml o .gml.gz de la carpeta /gml_files que contiene una descripción del grafo ya generado.
# 1 = No se usa un archivo .gml o .gml.gz, se genera el grafo desde cero con los .csv (por defecto).

save_csv_mod = 0
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
#Estas tablas deben estar creadas ya en el SQL. El nombre de las primeras se genera automáricamente dentro de la librería. 
#tabla_ct_nodos = "OUTPUT_8417_MIRAMONTE_NODOS"
#tabla_ct_trazas = "OUTPUT_8417_MIRAMONTE_TRAZAS"
#tabla_ct_nodos = "OUTPUT_1462_XUSTAS_NODOS"
#tabla_ct_trazas = "OUTPUT_1462_XUSTAS_TRAZAS"
tabla_cts_general = "OUTPUT_PERDIDAS_AGREGADOS_CT"
save_ddbb = 0
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
            # print(Nombre_CT)
            #if len(Nombre_CT) <= 1: 
            if Nombre_CT == 'FIN':
                break
            #id_ct = str(int(reader.readline().splitlines()[0])).replace('.0','')
            id_ct = int(reader.readline().splitlines()[0])
            # print(id_ct)
            
            #if len(Nombre_CT) > 1:
            if Nombre_CT != 'FIN':
                #Ruta y nombre del archivo de logout.
                ruta_log = ruta_raiz + 'log_files/Log_' + Nombre_CT.replace(' ', '_') + '_' + str(id_ct) + '_DEPERTEC.log'

                # while fecha_datetime < fecha_fin + datetime.timedelta(days=1):
                    # try:
                ga.Solve_Graph(fecha_ini=fecha_ini, fecha_fin=fecha_fin, Nombre_CT=Nombre_CT, id_ct=id_ct, archivo_topologia=archivo_topologia, archivo_traza=archivo_traza, archivo_ct_cups=archivo_ct_cups, ruta_cch=ruta_cch, archivo_config=archivo_config, ruta_log=ruta_log, V_Linea_400=V_Linea_400, V_Linea_230 = V_Linea_230, X_cable=X_cable, temp_cables=temp_cables, use_gml_file=use_gml_file, save_csv_mod=save_csv_mod, save_plt_graph=save_plt_graph, save_ddbb=save_ddbb, tabla_cts_general=tabla_cts_general, log_mode=log_mode, upper_limit=upper_limit, lower_limit=lower_limit)
                    # except:
                    #     cont_errores += 1
                    #     print('ERROR')
                    #     print(cont_errores)
                    # fecha_datetime = fecha_datetime + datetime.timedelta(days=1)
        except:
            pass
            # break
        
        

# fecha_datetime = fecha_ini
# while fecha_datetime < fecha_fin + datetime.timedelta(days=1):
# #    print(fecha_datetime)
#     ##Llamar a la función con todos los parámetros admitidos: 
#     # ga.Solve_Graph(fecha_datetime, Nombre_CT, id_ct, ruta_raiz, archivo_topologia, archivo_traza, archivo_ct_cups, ruta_cch, archivo_config, ruta_log, V_Linea, X_cable, temp_cables,  save_plt_graph, guardado_bbdd, tabla_cts_general=tabla_cts_general, log_mode)
#     ga.Solve_Graph(fecha_datetime=fecha_datetime, Nombre_CT=Nombre_CT, id_ct=id_ct, ruta_raiz=ruta_raiz, archivo_topologia=archivo_topologia, archivo_traza=archivo_traza, archivo_ct_cups=archivo_ct_cups, ruta_cch=ruta_cch, archivo_config=archivo_config, ruta_log=ruta_log, V_Linea=V_Linea, X_cable=X_cable, temp_cables=temp_cables, use_gml_file=use_gml_file, save_csv_mod=save_csv_mod, save_plt_graph=save_plt_graph, guardado_bbdd=guardado_bbdd, tabla_cts_general=tabla_cts_general, log_mode=log_mode)
    
#     ##Llamar a la función con únicamente con los parámetros básicos: 
# #    ga.Solve_Graph(fecha_datetime, Nombre_CT, ruta_raiz, archivo_topologia, archivo_traza, archivo_traza_mod, archivo_ct_cups, ruta_cch, archivo_config, ruta_log)

#     fecha_datetime = fecha_datetime + datetime.timedelta(days=1)






#https://www.cienciadedatos.net/documentos/py05_logging_con_python.html
#https://docs.python.org/es/dev/howto/logging.html
#https://www.ionos.es/digitalguide/paginas-web/desarrollo-web/logging-de-python/
    





