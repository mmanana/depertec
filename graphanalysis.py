# -*- coding: utf-8 -*-
"""
    File name: graphanalysis.py
    Author: Mario Mañana, David Carriles, GTEA
    Date created: 6 Aug 2020
    Date last modified: 1 Jul 2021
    Python Version: 3.6
    
    Library for creating an Electric Power System Graph and solve losses.
"""


##############################################################################
## GRAPH DEFINITION AND LOSSES ANALYSIS.
##############################################################################
# from IPython.display import Image
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
import math
import pyodbc 
import sys, os
import time
import datetime
import logging
import os.path as path

import cable # UC Library for Line Loss Analysis and Calculation of Electric Power Systems. Add in the same folder


class Solve_Graph:
    """
    
    Library for creating an Electric Power System Graph and solve losses. 
    Possibility to save results in an SQL DataBase.
    Required cable.py function in the same folder, as well as the code that calls this function.
    
    
    Parameters:
    ----------
    fecha_ini # Year, Month, Day to start the losses calculation [datetime.datetime]
    fecha_fin # Year, Month, Day to finish the losses calculation [datetime.datetime]
    Nombre_CT : CT name [Ej.: 'XUSTAS']
    id_ct: CT ID [Ej.: 1462]
    ruta_raiz : Project folder [F:\\GTEA\\DEPERTEC\\Grafo\\]
    archivo_topologia : CT nodes file [.csv]
    archivo_traza : CT edges file [.csv]
    archivo_ct_cups : CUPS-CT relation file [.csv]
    ruta_cch : Load curves folder
    archivo_config : SQL configuration file [.txt]
    ruta_log : Logging debug file [.log]
    
    
    
    Other parameters (defined by default):
    ----------
    V_Linea_400 : 400.0 # Line voltage [V] type 1
    V_Linea_230 : 230.0 # Line voltage [V] type 2
    X_cable : 0.0 # Line reactance [Ohms/km]
    temp_cables : 20.0 # Cable temperature [ºC]
    use_gml_file : 1 # Use .gml or .gml.gz file from /gml_files folder with a defined graph [0: Use file in /gml_files folder. 1 (default): Do not use file and generate a new graph and a new .gml.gz file]
    save_csv_mod : 1 # Save .csv modified files used to generate the graph configuration parameter [0: Save files in /csv_files folder. 1 (default): Do not save files]
    save_plt_graph : 1 # Save graph figures configuration parameter [0: Save images in /images_files folder. 1 (default): Do not save images]
    save_ddbb : 3 # SQL save results method configuration [0: Save all results. 1: Save only general results in 'tabla_cts_general'. 2: Save results only in CT tables. 3: Do not save results]
    tabla_cts_general : OUTPUT_PERDIDAS_AGREGADOS_CT # SQL general table name.
    log_mode : logging.INFO # Change logging mode in the ruta_log file. Change DEBUG, INFO, ERROR, WARNING, CRITICAL.
    
    """
    # fecha_ini # Year, Month, Day to start the losses calculation [datetime.datetime]
    # fecha_fin # Year, Month, Day to finish the losses calculation [datetime.datetime]
    # Nombre_CT  # CT name [Ej.: 'XUSTAS']
    # ruta_raiz # Project folder
    # archivo_topologia  # CT nodes file [.csv]
    # archivo_traza  # CT edges file [.csv]
    # archivo_ct_cups   # CUPS-CT relation file [.csv]
    # ruta_cch  # Load curves folder
    # archivo_config  # SQL configuration file [.txt]
    # ruta_log   # Logging debug file [.log]
    
    ### Default parameters
    # V_Linea_400 = 400.0 # Line voltage [V]
    # X_cable = 0.0 # Line reactance [Ohms/km]
    # temp_cables = 20.0 # Cable temperature [ºC]
    # use_gml_file = 1 # Use a .gml file generated previously with a CT graph definition [0: Do not use a .gml file, create a new graph and save it as .gml. 1: Use a .gml file and do not create a new graph to simplify the code]
    # save_csv_mod = 1  # Save .csv modified files used to generate the graph configuration parameter [0: Save files in /csv_files folder. 1 (default): Do not save files]
    # save_plt_graph = 1  # Save graph figures configuration parameter [0: Save images in /images_files folder. 1 (default): Do not save images]
    # save_ddbb = 3 # SQL save results method configuration [0: Save all results. 1: Save only general results in 'tabla_cts_general'. 2: Save results only in CT tables. 3: Do not save results]
    # tabla_cts_general = 'OUTPUT_PERDIDAS_AGREGADOS_CT' # SQL general table name
    # log_mode = 'logging.INFO' # Change logging mode in the ruta_log file. Change DEBUG, INFO, ERROR, WARNING, CRITICAL.
    # upper_limit = 10 # Default=10. Upper limit value to consider the result correct.
    # lower_limit = 10 # Default=10. Lower limit value to consider the result correct.

    
    version = r'Solve Electric Power System Graph Losses Library. v15'
    fecha_ini: datetime.datetime
    fecha_fin: datetime.datetime
    Nombre_CT: str
    id_ct: int
    ruta_raiz: str
    archivo_topologia: str
    archivo_traza: str
    archivo_ct_cups: str
    ruta_cch: str
    archivo_config: str
    ruta_log: str
    V_Linea_400: float
    V_Linea_230: float
    X_cable: float
    temp_cables: float
    use_gml_file: int
    save_csv_mod: int
    save_plt_graph: int
    save_ddbb: int
    tabla_cts_general: str
    log_mode: str
    upper_limit: float
    lower_limit: float
    
    
    def __init__( self, fecha_ini, fecha_fin, Nombre_CT, id_ct, archivo_topologia, archivo_traza, archivo_ct_cups, ruta_cch, archivo_config, ruta_log, V_Linea_400=400.0, V_Linea_230=230, X_cable=0, temp_cables=20, use_gml_file=1, save_csv_mod=1, save_plt_graph=1, save_ddbb=3, tabla_cts_general='OUTPUT_PERDIDAS_AGREGADOS_CT', log_mode = 'logging.INFO', upper_limit=10, lower_limit=10):
        self.fecha_ini = fecha_ini
        self.fecha_fin = fecha_fin
        self.Nombre_CT = Nombre_CT
        self.id_ct = id_ct
        self.ruta_raiz = os.path.dirname(os.path.abspath(__file__)) + '\\'         #Directorio donde está el .py, las librerías y los archivos
        self.archivo_topologia = archivo_topologia
        self.archivo_traza = archivo_traza
        self.archivo_ct_cups = archivo_ct_cups
        self.ruta_cch = ruta_cch
        self.archivo_config = archivo_config
        self.ruta_log = ruta_log
        self.V_Linea_400 = V_Linea_400
        self.V_Linea_230 = V_Linea_230
        self.X_cable = X_cable
        self.temp_cables = temp_cables
        self.use_gml_file = use_gml_file
        self.save_csv_mod = save_csv_mod
        self.save_plt_graph = save_plt_graph
        self.save_ddbb = save_ddbb
        self.tabla_cts_general = tabla_cts_general
        self.log_mode = log_mode
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit
        
        print(Nombre_CT)
        print('ID_CT: ' + str(id_ct))
        print('V_Linea_400: ' + str(V_Linea_400) + ', V_Linea_230: ' + str(V_Linea_230))
        print('X_cable: ' + str(X_cable))
        print('Temp_cables: ' + str(temp_cables))
        # print(archivo_topologia)
        # print(archivo_traza)
        # print(archivo_ct_cups)
        # print(ruta_cch)
        # print(archivo_config)
        # print(ruta_log)
        print('use_gml_file: ' + str(use_gml_file))
        print('save_csv_mod: ' + str(save_csv_mod))
        print('save_plt_graph: ' + str(save_plt_graph))
        print(tabla_cts_general)
        print('save_ddbb: ' + str(save_ddbb))
        print(log_mode)
        
        # graph_data_error = main(self)
        main(self)
            
         
       
            
        



##############################################################################
## FUNCTIONS DEFINITION
##############################################################################
    
    def update_graph_data_error(self, graph_data_error):
        """
        
        Función para actualizar el parámetro graph_data_error que indica la fiabilidad del grafo generado para cada CT.
        Importante: Se intenta actualizar el valor existente o, si no está registrado previamente ese CT, se añade a la lista. En caso de que el .csv no exista en la carpeta raíz del proyecto se crea.
        
        Parámetros
        ----------
        graph_data_error : Variable para determinar la viabilidad del grafo en función de la calidad de los datos que lo definen.
            
        
        Retorno
        -------
        """
        logger = logging.getLogger('update_graph_data_error')
        try:
            #Se actualiza en el .csv el valor de la variable graph_data_error para este CT.
            graph_data_error_file = self.ruta_raiz + 'Graph_data_error.csv'
            #Se comprueba si existe el archivo .csv que contiene la variable graph_data_error de los CTs simulados. Si no existe se crea uno nuevo.
            if path.exists(graph_data_error_file):
                #Hay veces que el archivo está creado pero está vacío y da error de lectura porque no tiene columnas.
                try:
                    graph_data_error_df = pd.read_csv(graph_data_error_file, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal=',')
                    #Se comprueba si existe un registro para este CT.
                    ct_prov = graph_data_error_df.loc[(graph_data_error_df['Nombre_CT'] == str(self.Nombre_CT)) & (graph_data_error_df['ID_CT'] == self.id_ct)].reset_index(drop=True)
                    if len(ct_prov) > 0:
                        #Se actualizan los valores de fechahora y graph_data_error.
                        graph_data_error_df.loc[(graph_data_error_df['Nombre_CT'] == str(self.Nombre_CT)) & (graph_data_error_df['ID_CT'] == (self.id_ct)), 'Fechahora'] = str(time.strftime("%d/%m/%y")) + ' - ' + str(time.strftime("%H:%M:%S"))
                        graph_data_error_df.loc[(graph_data_error_df['Nombre_CT'] == str(self.Nombre_CT)) & (graph_data_error_df['ID_CT'] == (self.id_ct)), 'Graph_data_error'] = graph_data_error
                        graph_data_error_df.to_csv(graph_data_error_file, index = False, encoding='Latin9', sep=';', decimal=',')
                    else:
                        graph_data_error_df = graph_data_error_df.append({'Fechahora': str(time.strftime("%d/%m/%y")) + ' - ' + str(time.strftime("%H:%M:%S")), 'Nombre_CT': str(self.Nombre_CT), 'ID_CT': str(self.id_ct), 'Graph_data_error': str(graph_data_error)}, ignore_index=True)
                        graph_data_error_df.to_csv(graph_data_error_file, index = False, encoding='Latin9', sep=';', decimal=',')
                        del graph_data_error_df
                except:
                    graph_data_error_df = pd.DataFrame(columns=['Fechahora', 'Nombre_CT', 'ID_CT', 'Graph_data_error'])
                    graph_data_error_df = graph_data_error_df.append({'Fechahora': str(time.strftime("%d/%m/%y")) + ' - ' + str(time.strftime("%H:%M:%S")), 'Nombre_CT': str(self.Nombre_CT), 'ID_CT': str(self.id_ct), 'Graph_data_error': str(graph_data_error)}, ignore_index=True)
                    graph_data_error_df.to_csv(graph_data_error_file, index = False, encoding='Latin9', sep=';', decimal=',')
                    del graph_data_error_df
            else:
                graph_data_error_df = pd.DataFrame(columns=['Fechahora', 'Nombre_CT', 'ID_CT', 'Graph_data_error'])
                graph_data_error_df = graph_data_error_df.append({'Fechahora': str(time.strftime("%d/%m/%y")) + ' - ' + str(time.strftime("%H:%M:%S")), 'Nombre_CT': str(self.Nombre_CT), 'ID_CT': str(self.id_ct), 'Graph_data_error': str(graph_data_error)}, ignore_index=True)
                graph_data_error_df.to_csv(graph_data_error_file, index = False, encoding='Latin9', sep=';', decimal=',')
                del graph_data_error_df
            logger.debug('Graph_data_error actualizado correctamente en el .csv.')
        except:
            logger.error('Error al guardar graph_data_error en el .csv. Mantener el archivo cerrado para poder sobreescribir.')
        return


    def create_graph_dataframes(self, graph_data_error, archivo_topologia, archivo_traza, archivo_ct_cups):
        """
        
        Función para leer los .csv con la información de los CTs, filtrar los datos para el CT de análisis, adecuar el formato de los datos y corregir los posibles errores en los mismos. Emplea la librería cables.py para comprobar que el tipo de cable de cada traza existe en el .xml de cables y, si no existe, corregir errores en el tipo de cable.
        
        Parámetros
        ----------
        graph_data_error : Variable para determinar la viabilidad del grafo en función de la calidad de los datos que lo definen.
        archivo_topologia : .csv con la definición de nodos de todos los CTs.
        archivo_traza : .csv con la definición de trazas de todos los CTs.
        archivo_ct_cups : .csv con la definición de CUPS asociados a todos los CTs.
            
        
        Retorno
        -------
        graph_data_error : Valor actualizado.
        df_nodos_ct : DataFrame con los nodos del CT a analizar con los posibles errores corregidos.
        df_traza_ct : DataFrame con las trazas del CT a analizar con los posibles errores corregidos.
        df_ct_cups_ct : DataFrame con la relación de CUPS del CT a analizar con los posibles errores corregidos.
        df_matr_dist : DataFrame con los CUPS del CT asociados al nodo más cercano que coincide con el trafo al que está conectado.
        LBT_ID_list : Lista de las LBTs que componen el grafo.
        cups_agregado_CT : DataFrame con los CUPS de la cabecera del CT.
        """
        logger = logging.getLogger('create_graph_dataframes')
        ##############################################################################
        ## Creación de DataFrames con las tablas para definir el grafo. Se filtran por el nombre e ID del CT nada más leer el archivo completo.
        ## En caso de leer el grafo de un .gml o gml.gz se omite este paso.
        ##############################################################################
        #Archivo de nodos
        df_nodos = pd.read_csv(self.archivo_topologia, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal=',')
        df_nodos_ct = df_nodos[(df_nodos['CT_NOMBRE'] == self.Nombre_CT) & (df_nodos['CT'] == self.id_ct)].reset_index(drop=True).copy()
        #Se eliminan todos los valores NaN que pueda haber en las columnas tipo nodo y coordenadas, reemplazándolos por un caracter vacío.
        df_nodos_ct.TIPO_NODO.fillna('', inplace=True) 
        df_nodos_ct.NUDO_X.fillna('', inplace=True)
        df_nodos_ct.NUDO_Y.fillna('', inplace=True)
        df_nodos_ct['TRAFO'] = df_nodos_ct['TRAFO'].str.upper()
        
        #Es necesario cambiar algún tipo de datos para que no lo considere como número e incluya decimales .0
        borrar_fila = [] #Vector de posibles valores a eliminar del DF, para no influir en el index y borrarlos todos después del ciclo.
        for index,row in df_nodos_ct.iterrows():
            df_nodos_ct.loc[index, 'ID_NODO'] = str(row.ID_NODO).replace('.0', '').replace(' ', '')
            df_nodos_ct.loc[index, 'LBT_NOMBRE'] = str(row.LBT_NOMBRE).replace('.0', '').replace(' ', '')
            df_nodos_ct.loc[index, 'LBT_ID'] = str(row.LBT_ID).replace('.0', '').replace(' ', '')
            df_nodos_ct.loc[index, 'TRAFO'] = str(row.TRAFO).replace(' ', '').replace('.','').replace(',','')
            #Se comprueba que el ID_NODO es mayor que 0 y no está vacío.
            try:
                if int(row.ID_NODO) > 0:
                    aa='Todo ok'
                    del aa
                else:
                    df_nodos_ct.loc[index, 'ID_NODO'] = '' #Se define un valor vacío para no guardar un NaN
                    logger.error('NODOS: Posible error en el ID_NODO ' + str(row.ID_NODO))
            except:
                logger.error('NODOS: Posible error en el ID_NODO ' + str(row.ID_NODO) + '. Se borra de la lista y no se considera.')
                borrar_fila.append(index)
                # df_nodos_ct = df_nodos_ct.drop(index, axis=0)
                if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 1
                
            try:
                if int(row.LBT_NOMBRE) > 0:
                    aa='Todo ok'
                    del aa
                else:
                    logger.error('NODOS: Posible error en el LBT_NOMBRE ' + str(row.LBT_NOMBRE) + ' del ID_NODO ' + str(row.ID_NODO))
            except:
                logger.error('NODOS: Posible error en el LBT_NOMBRE ' + str(row.LBT_NOMBRE) + ' del ID_NODO ' + str(row.ID_NODO))
            
            try:
                if int(row.LBT_ID) > 0:
                    aa='Todo ok'
                    del aa
                else:
                    logger.error('NODOS: Posible error en el LBT_ID ' + str(row.LBT_ID) + ' del ID_NODO ' + str(row.ID_NODO))
                    if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 1
            except:
                logger.error('NODOS: Posible error en el LBT_ID ' + str(row.LBT_ID) + ' del ID_NODO ' + str(row.ID_NODO))
                if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 1
                        
        #Se borran las filas:
        for row in borrar_fila:
            df_nodos_ct = df_nodos_ct.drop(index, axis=0)
        del borrar_fila
        #Se reinicia el índice del DF.
        df_nodos_ct = df_nodos_ct.reset_index(drop=True)
                    
        
        
        
        try:
            df_nodos_ct['NUDO_X'] = df_nodos_ct['NUDO_X'].astype('float')
            df_nodos_ct['NUDO_Y'] = df_nodos_ct['NUDO_Y'].astype('float')
        except:
            logger.error('Error al intentar asegurar que todas las coordenadas X, Y del archivo Topología son "float".')

                    
        
        if len(df_nodos_ct) > 0:
            logger.debug('.CSV de nodos leído y filtrado. df_nodos_ct = ' + str(len(df_nodos_ct)))
        else:
            logger.debug('.CSV de nodos leído y filtrado. df_nodos_ct = ' + str(len(df_nodos_ct)))
            logger.warning('Error al filtrar los nodos por el nombre del CT indicado: ' + self.Nombre_CT + '. Ningún valor encontrado. El nombre debe corresponder con la columna "CT_NOMBRE" del .CSV, que el archivo indicado es el adecuado o si no se han definido nodos de la red.')
            if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 1
        del df_nodos
        
        
        #Archivo de trazas
        df_traza = pd.read_csv(self.archivo_traza, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal = ',')
        df_traza_ct = df_traza[(df_traza['CT_NOMBRE'] == self.Nombre_CT) & (df_traza['CT'] == self.id_ct)].reset_index(drop=True).copy()
        #Se elimintan todos los valores NaN que pueda haber en la columna trafo y cable
        df_traza_ct.TRAFO.fillna('', inplace=True)
        df_traza_ct.CABLE.fillna('', inplace=True)
        df_traza_ct.TIPO_UBICACION.fillna('', inplace=True)
        
        #Se adaptan los nombres de los cables, trafo y tipo de ubicación        
        df_traza_ct['TRAFO'] = df_traza_ct['TRAFO'].str.upper().str.replace(' ', '')
        df_traza_ct['CABLE'] = df_traza_ct['CABLE'].str.upper().str.replace(' ', '_').str.replace(',','.')
        df_traza_ct['TIPO_UBICACION'] = df_traza_ct['TIPO_UBICACION'].str.upper().str.replace(' ', '')
        
        #Se crea una columna que contenga el nombre original del CABLE, para revisiones posteriores, ya que la columna CABLE se reescribirá en caso de errores
        temp=[]
        temp = df_traza_ct['CABLE'].apply(str)
        df_traza_ct['CABLE_ORIG'] = temp
        del temp
        
        #Se asegura el formato de varios parámetros y se crea la columna de longitud de traza.
        df_traza_ct = df_traza_ct.reindex(columns = df_traza_ct.columns.tolist() + ["Longitud"]) 
        borrar_fila = [] #Vector de posibles valores a eliminar del DF, para no influir en el index y borrarlos todos después del ciclo.
        trazas_cero = 0
        for index,row in df_traza_ct.iterrows():
            df_traza_ct.loc[index, 'NODO_ORIGEN'] = str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '')
            df_traza_ct.loc[index, 'NODO_DESTINO'] = str(row.NODO_DESTINO).replace('.0', '').replace(' ', '')
            df_traza_ct.loc[index, 'LBT_ID'] = str(row.LBT_ID).replace('.0', '').replace(' ', '')
            df_traza_ct.loc[index, 'TRAFO'] =str(row.TRAFO).replace(' ', '').replace('.','').replace(',','')
            
            #Se comprueba que nodo origen y nodo destino de cada traza es un entero mayor que 0. Se han visto 'nan' y se eliminan las filas en este caso.
            try:
                if int(row.NODO_ORIGEN) > 0 and int(row.NODO_DESTINO) > 0:
                    aa='Todo ok'
                    del aa
                else:
                    logger.error('TRAZAS: Posible error en el NODO_ORIGEN ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + ' NODO_DESTINO ' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', ''))
                    if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 2
            except:
                logger.error('TRAZAS: Posible error en el NODO_ORIGEN ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + ' NODO_DESTINO ' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se borra esta fila del DF.')
                # df_traza_ct = df_traza_ct.drop(index, axis=0)
                if index not in borrar_fila:
                    borrar_fila.append(index)
                if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 2
                continue
            
            #Se comprueban las coordenadas de los nodos origen y destino. Si no tienen el formato adecuado se busca en el DF de nodos si está el nodo con las coordenadas correctas.
            try:
                df_traza_ct.loc[index, 'X_ORIGEN'] = float(str(row.X_ORIGEN).replace(',','.').replace(' ', ''))
                if df_traza_ct.loc[index, 'X_ORIGEN'] > 0:
                    aa='Todo ok'
                    del aa
                else:
                    #Se Busca el ID_NODO en el DF de nodos y si se encuentra se ordena de mayor a menor, por si hay celdas vacías coger la primera que tenga coordenadas.
                    x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_ORIGEN']].sort_values('NUDO_X', ascending=False).reset_index(drop=True)#['NUDO_X']
                    if len(x_prov) > 0:
                        try:
                            if float(x_prov['NUDO_X'][0]) > 0: #Posibles errores si está vacío solo con ''
                                df_traza_ct.loc[index, 'X_ORIGEN'] = float(x_prov['NUDO_X'][0])
                                logger.warning('TRAZAS: Error de coordenada X_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_ORIGEN) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'X_ORIGEN']))
                        except:
                            df_traza_ct.loc[index, 'X_ORIGEN'] = 0
                            logger.warning('TRAZAS: Error de coordenada X_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
                        del x_prov
                    else:
                        df_traza_ct.loc[index, 'X_ORIGEN'] = 0
                        logger.warning('TRAZAS: Error de coordenada X_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
            except:
                # x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_ORIGEN']].reset_index(drop=True)['NUDO_X']
                x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_ORIGEN']].sort_values('NUDO_X', ascending=False).reset_index(drop=True)#['NUDO_X']
                if len(x_prov) > 0:
                    try:
                        if float(x_prov['NUDO_X'][0]) > 0: #Posibles errores si está vacío solo con ''
                            df_traza_ct.loc[index, 'X_ORIGEN'] = float(x_prov['NUDO_X'][0])
                            logger.warning('TRAZAS: Error de coordenada X_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_ORIGEN) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'X_ORIGEN']))
                    except:
                        df_traza_ct.loc[index, 'X_ORIGEN'] = 0
                        logger.warning('TRAZAS: Error de coordenada X_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
                    del x_prov

                else:
                    df_traza_ct.loc[index, 'X_ORIGEN'] = 0
                    logger.warning('TRAZAS: Error de coordenada X_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
                      
            
            ######        
            try:
                df_traza_ct.loc[index, 'Y_ORIGEN'] = float(str(row.Y_ORIGEN).replace(',','.').replace(' ', ''))
                if df_traza_ct.loc[index, 'Y_ORIGEN'] > 0:
                    aa='Todo ok'
                    del aa
                else:
                    y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_ORIGEN']].sort_values('NUDO_Y', ascending=False).reset_index(drop=True)#['NUDO_Y']
                    if len(y_prov) > 0:
                        try:
                            if float(y_prov['NUDO_Y'][0]) > 0: #Posibles errores si está vacío solo con ''
                                df_traza_ct.loc[index, 'Y_ORIGEN'] = float(y_prov['NUDO_Y'][0])
                                logger.warning('TRAZAS: Error de coordenada Y_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_ORIGEN) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'Y_ORIGEN']))
                        except:
                            df_traza_ct.loc[index, 'Y_ORIGEN'] = 0
                            logger.warning('TRAZAS: Error de coordenada Y_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
                        del y_prov
                    else:
                        df_traza_ct.loc[index, 'Y_ORIGEN'] = 0
                        logger.warning('TRAZAS: Error de coordenada Y_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
            except:
                # y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_ORIGEN']].reset_index(drop=True)['NUDO_Y']
                y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_ORIGEN']].sort_values('NUDO_Y', ascending=False).reset_index(drop=True)#['NUDO_Y']
                if len(y_prov) > 0:
                    try:
                        if float(y_prov['NUDO_Y'][0]) > 0: #Posibles errores si está vacío solo con ''
                            df_traza_ct.loc[index, 'Y_ORIGEN'] = float(y_prov['NUDO_Y'][0])
                            logger.warning('TRAZAS: Error de coordenada Y_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_ORIGEN) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'Y_ORIGEN']))
                    except:
                        df_traza_ct.loc[index, 'Y_ORIGEN'] = 0
                        logger.warning('TRAZAS: Error de coordenada Y_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
                    del y_prov
                else:
                    df_traza_ct.loc[index, 'Y_ORIGEN'] = 0
                    logger.warning('TRAZAS: Error de coordenada Y_ORIGEN para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_ORIGEN) + ', definido valor 0 al no poder resolver el error.')
            
       
            ######        
            try:
                df_traza_ct.loc[index, 'X_DESTINO'] = float(str(row.X_DESTINO).replace(',','.').replace(' ', ''))
                if df_traza_ct.loc[index, 'X_DESTINO'] > 0:
                    aa='Todo ok'
                    del aa
                else:
                    # x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].reset_index(drop=True)['NUDO_X']
                    x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].sort_values('NUDO_X', ascending=False).reset_index(drop=True)#['NUDO_Y']
                    if len(x_prov) > 0:
                        try:
                            if float(x_prov['NUDO_X'][0]) > 0: #Posibles errores si está vacío solo con ''
                                df_traza_ct.loc[index, 'X_DESTINO'] = float(x_prov['NUDO_X'][0])
                                logger.warning('TRAZAS: Error de coordenada X_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_DESTINO) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'X_DESTINO']))
                        except:
                            df_traza_ct.loc[index, 'X_DESTINO'] = 0
                            logger.warning('TRAZAS: Error de coordenada X_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_DESTINO) + ', definido valor 0 al no poder resolver el error.')
                        del x_prov
                    else:
                        df_traza_ct.loc[index, 'X_DESTINO'] = 0
                        logger.warning('TRAZAS: Error de coordenada X_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_DESTINO) + ', definido valor 0 al no poder resolver el error.')
            except:
                # x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].reset_index(drop=True)['NUDO_X']
                x_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].sort_values('NUDO_X', ascending=False).reset_index(drop=True)#['NUDO_Y']
                if len(x_prov) > 0:
                    try:
                        if float(x_prov['NUDO_X'][0]) > 0: #Posibles errores si está vacío solo con ''
                            df_traza_ct.loc[index, 'X_DESTINO'] = float(x_prov['NUDO_X'][0])
                            logger.warning('TRAZAS: Error de coordenada X_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_DESTINO) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'X_DESTINO']))
                    except:
                        df_traza_ct.loc[index, 'X_DESTINO'] = 0
                        logger.warning('TRAZAS: Error de coordenada X_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_DESTINO) + ', definido valor 0 al no poder resolver el error.')
                    del x_prov
                    
                else:
                    df_traza_ct.loc[index, 'X_DESTINO'] = 0
                    logger.warning('TRAZAS: Error de coordenada X_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. X original: ' + str(row.X_DESTINO) + ', definido valor 0 al no poder resolver el error.')
            
       
            ######        
            try:
                df_traza_ct.loc[index, 'Y_DESTINO'] = float(str(row.Y_DESTINO).replace(',','.').replace(' ', ''))
                if df_traza_ct.loc[index, 'Y_DESTINO'] > 0:
                    aa='Todo ok'
                    del aa
                else:
                    # y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].reset_index(drop=True)['NUDO_Y']
                    y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].sort_values('NUDO_Y', ascending=False).reset_index(drop=True)#['NUDO_Y']
                    if len(y_prov) > 0:
                        try:
                            if float(y_prov['NUDO_Y'][0]) > 0: #Posibles errores si está vacío solo con ''
                                df_traza_ct.loc[index, 'Y_DESTINO'] = float(y_prov['NUDO_Y'][0])
                                logger.warning('TRAZAS: Error de coordenada Y_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_DESTINO) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'Y_DESTINO']))
                        except:
                            df_traza_ct.loc[index, 'Y_DESTINO'] = 0
                            logger.warning('TRAZAS: Error de coordenada Y_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_DESTINO) + ', definido valor 0 al no poder resolver el error.')
                        del y_prov                       
                    else:
                        df_traza_ct.loc[index, 'Y_DESTINO'] = 0
                        logger.warning('TRAZAS: Error de coordenada Y_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_DESTINO) + ', definido valor 0 al no poder resolver el error.')
            except:
                # y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].reset_index(drop=True)['NUDO_Y']
                y_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO'] == df_traza_ct.loc[index, 'NODO_DESTINO']].sort_values('NUDO_Y', ascending=False).reset_index(drop=True)#['NUDO_Y']
                if len(y_prov) > 0:
                    try:
                        if float(y_prov['NUDO_Y'][0]) > 0: #Posibles errores si está vacío solo con ''
                            df_traza_ct.loc[index, 'Y_DESTINO'] = float(y_prov['NUDO_Y'][0])
                            logger.warning('TRAZAS: Error de coordenada Y_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_DESTINO) + ', encontrado en el DF de nodos el valor: ' + str(df_traza_ct.loc[index, 'Y_DESTINO']))
                    except:
                        df_traza_ct.loc[index, 'Y_DESTINO'] = 0
                        logger.warning('TRAZAS: Error de coordenada Y_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_DESTINO) + ', definido valor 0 al no poder resolver el error.')
                    del y_prov   
                else:
                    df_traza_ct.loc[index, 'Y_DESTINO'] = 0
                    logger.warning('TRAZAS: Error de coordenada Y_DESTINO para el enlace ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Y original: ' + str(row.Y_DESTINO) + ', definido valor 0 al no poder resolver el error.')
            
            
            
            try:
                #Hay que comprobar que todas las coordenadas son mayores que 0. Hay casos de trazas con mismas coordenadas de origen y destino y no hay que considerarlo como valor erróneo en el cálculo de trazas con longitud 0.
                if float(df_traza_ct.loc[index, 'X_ORIGEN']) > 0 and float(df_traza_ct.loc[index, 'Y_ORIGEN']) > 0 and float(df_traza_ct.loc[index, 'X_DESTINO']) > 0 and float(df_traza_ct.loc[index, 'Y_DESTINO']) > 0:
                    long_calc = math.sqrt((float(df_traza_ct.loc[index, 'X_DESTINO']) - float(df_traza_ct.loc[index, 'X_ORIGEN']))**2 + (float(df_traza_ct.loc[index, 'Y_DESTINO']) - float(df_traza_ct.loc[index, 'Y_ORIGEN']))**2)
                else:
                    long_calc = 0
                    if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 1
                    logger.warning('TRAZAS: Error al calcular la longitud para la traza ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Valor obtenido: ' + str(long_calc) + '. Asignado valor 0.')
                    if index not in borrar_fila:
                        trazas_cero += 1
                        # borrar_fila.append(index)
                    
                if long_calc >= 0:
                    df_traza_ct.loc[index, 'Longitud'] = long_calc
                else:
                    df_traza_ct.loc[index, 'Longitud'] = 0
                    logger.warning('TRAZAS: Error al calcular la longitud para la traza ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Valor obtenido: ' + str(long_calc) + '. Asignado valor 0.')
                    if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 1
            except:
                df_traza_ct.loc[index, 'Longitud'] = 0
                logger.warning('TRAZAS: Error de código al calcular longitud para la traza ' + str(df_traza_ct.loc[index, 'NODO_ORIGEN']) + ' - ' + str(df_traza_ct.loc[index, 'NODO_DESTINO']) + '. Valor obtenido: ' + str(long_calc) + '. Asignado valor 0.')
                if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 1
                #continue
            
            try:
                if int(row.LBT_ID) > 0:
                    aa='Todo ok'
                    del aa
                else:
                    logger.warning('TRAZAS: Posible error en el LBT_ID ' + str(row.LBT_ID).replace('.0', '').replace(' ', '') + ' del NODO_ORIGEN ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + ' NODO_DESTINO ' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', ''))
                    if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 2
            except:
                logger.warning('TRAZAS: Posible error en el LBT_ID ' + str(row.LBT_ID).replace('.0', '').replace(' ', '') + ' del NODO_ORIGEN ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + ' NODO_DESTINO ' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', ''))
                if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 2
            
            
                
            #Se comprueba si el tipo de cable está en el archivo .xml de la librería 'Cable'. Si no está s intenta corregir con otro cable de ubicación similar o si no es posible se tomarán valores por defecto de la librería.
            try:
                Cable = cable.Conductor()
                Found_cable = Cable.fload_library(str(row.CABLE_ORIG))
                #Cuidado con las celdas que puedan estar vacías. Si no hay cable Found_cable sale como 1
                if Found_cable == 0 or len(str(row.CABLE_ORIG)) <= 3:
                    # Se busca entre el resto de tipos de cable de esa misma ubicación si alguno está en la librería
                    prov = df_traza_ct.loc[df_traza_ct.TIPO_UBICACION == row.TIPO_UBICACION].drop_duplicates(subset=['CABLE_ORIG', 'TIPO_UBICACION'], keep = 'first').reset_index(drop=True)
                    if len(prov) > 0:
                        for indice, fila in prov.iterrows():
                            Cable_2 = cable.Conductor()
                            Found_cable_2 = Cable_2.fload_library(str(fila.CABLE_ORIG))
                            if Found_cable_2 == 1:
                                df_traza_ct.loc[index, 'CABLE'] = str(fila.CABLE_ORIG)
                                print('Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se ha utilizado el cable ' + str(fila.CABLE_ORIG) + ' encontrado como tipo ' + str(row.TIPO_UBICACION))
                                logger.warning('TRAZAS: Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se ha utilizado el cable ' + str(fila.CABLE_ORIG) + ' encontrado como tipo ' + str(row.TIPO_UBICACION))
                                if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                    graph_data_error = 1
                                break
                        #Si no se encuentra el tipo de cable según el tipo de ubicación se hace una búsqueda entre todos los tipos de cables que pueda haber en el CT y se coge el primero que esté en la librería.
                        if Found_cable_2 == 0:# and len(str(row.CABLE_ORIG)) <= 3:
                            prov2 = df_traza_ct.drop_duplicates(subset=['CABLE_ORIG'], keep = 'first').reset_index(drop=True)
                            if len(prov2) > 0:
                                for indice, fila in prov2.iterrows():
                                    Cable_3 = cable.Conductor()
                                    Found_cable_3 = Cable_3.fload_library(str(fila.CABLE_ORIG))
                                    if Found_cable_3 == 1 and len(str(fila.CABLE_ORIG)) >= 3:
                                        df_traza_ct.loc[index, 'CABLE'] = str(fila.CABLE_ORIG)
                                        print('Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se ha utilizado el cable ' + str(fila.CABLE_ORIG) + ' encontrado como tipo ' + str(fila.TIPO_UBICACION))
                                        logger.warning('TRAZAS: Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se ha utilizado el cable ' + str(fila.CABLE_ORIG) + ' encontrado como tipo ' + str(fila.TIPO_UBICACION))
                                        if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                            graph_data_error = 2
                                        break
                            del prov2, Cable_3, Found_cable_3
                            
                        del Cable_2, Found_cable_2
                    else:
                        del prov
                        #Si no se encuentra el tipo de cable según el tipo de ubicación se hace una búsqueda entre todos los tipos de cables que pueda haber en el CT y se coge el primero que esté en la librería.
                        prov = df_traza_ct.drop_duplicates(subset=['CABLE_ORIG'], keep = 'first').reset_index(drop=True)
                        if len(prov) > 0:
                            for indice, fila in prov.iterrows():
                                Cable_2 = cable.Conductor()
                                Found_cable_2 = Cable_2.fload_library(str(fila.CABLE_ORIG))
                                if Found_cable_2 == 1 and len(str(fila.CABLE_ORIG)) >= 3:
                                    df_traza_ct.loc[index, 'CABLE'] = str(fila.CABLE_ORIG)
                                    print('Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se ha utilizado el cable ' + str(fila.CABLE_ORIG) + ' encontrado como tipo ' + str(fila.TIPO_UBICACION))
                                    logger.warning('TRAZAS: Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + '. Se ha utilizado el cable ' + str(fila.CABLE_ORIG) + ' encontrado como tipo ' + str(fila.TIPO_UBICACION))
                                    if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                        graph_data_error = 2
                                    break
                            del Cable_2, Found_cable_2
                        else:
                            print('Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + ' tipo ' + str(row.TIPO_UBICACION) + '. Se consideran valores definidos por defecto en la librería "Cable".')
                            logger.warning('TRAZAS: Error al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' en la librería .xml. Enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', '') + ' tipo ' + str(row.TIPO_UBICACION) + '. Se consideran valores definidos por defecto en la librería "Cable".')
                            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                graph_data_error = 2
                        del prov
        
                del Cable, Found_cable
            except:
                logger.error('TRAZAS: Error desconocido al buscar el tipo de cable "' + str(row.CABLE_ORIG) + '" (' + str(row.TIPO_UBICACION) + ')' + ' del enlace ' + str(row.NODO_ORIGEN).replace('.0', '').replace(' ', '') + '-' + str(row.NODO_DESTINO).replace('.0', '').replace(' ', ''))
                if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 2
                    
        #Se borran las filas:
        for row in borrar_fila:
            df_traza_ct = df_traza_ct.drop(row, axis=0)
        del borrar_fila
            
        df_traza_ct = df_traza_ct.reset_index(drop=True)
        
        
        #Se comprueban las coordenadas del DF nodos
        for index,row in df_nodos_ct.iterrows():
            try:
                if float(row.NUDO_X) > 0:
                    aa='Todo ok'
                    del aa
            except:
                if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 1
                try:
                    x_prov1 = df_traza_ct.loc[df_traza_ct['NODO_ORIGEN'] == row.ID_NODO].sort_values('X_ORIGEN', ascending=False).reset_index(drop=True)['X_ORIGEN']#[0]
                    x_prov2 = df_traza_ct.loc[df_traza_ct['NODO_DESTINO'] == row.ID_NODO].sort_values('X_DESTINO', ascending=False).reset_index(drop=True)['X_DESTINO']#[0]
                    if len(x_prov1) > 0 and x_prov1[0] > 0:
                        df_nodos_ct.loc[index, 'NUDO_X'] = x_prov1[0]
                        del x_prov1, x_prov2
                    elif len(x_prov2) > 0 and x_prov2[0] > 0:
                        df_nodos_ct.loc[index, 'NUDO_X'] = x_prov2[0]
                        del x_prov1, x_prov2
                    else:
                        x_prov = df_nodos_ct.loc[df_nodos_ct['CT_X']>0].reset_index(drop=True)['CT_X'][0]
                        if x_prov >= 0:
                            df_nodos_ct.loc[index, 'NUDO_X'] = x_prov
                        else:
                            df_nodos_ct.loc[index, 'NUDO_X'] = 0
                        del x_prov1, x_prov2, x_prov
                except:
                    df_nodos_ct.loc[index, 'NUDO_X'] = 0
                logger.error('NODOS: Error de coordenada X (' + str(row.NUDO_X) + ') para el nodo ' + str(row.ID_NODO) + ' LBT_ID ' + str(row.LBT_ID))
                
            try:
                if float(row.NUDO_Y) > 0:
                    aa='Todo ok'
                    del aa
            except:
                if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 1
                try:
                    y_prov1 = df_traza_ct.loc[df_traza_ct['NODO_ORIGEN'] == row.ID_NODO].sort_values('Y_ORIGEN', ascending=False).reset_index(drop=True)['Y_ORIGEN']#[0]
                    y_prov2 = df_traza_ct.loc[df_traza_ct['NODO_DESTINO'] == row.ID_NODO].sort_values('Y_DESTINO', ascending=False).reset_index(drop=True)['Y_DESTINO']#[0]
                    if len(y_prov1) > 0 and y_prov1[0] > 0:
                        df_nodos_ct.loc[index, 'NUDO_Y'] = y_prov1[0]
                        del y_prov1, y_prov2
                    elif len(y_prov2) > 0 and y_prov2[0] > 0:
                        df_nodos_ct.loc[index, 'NUDO_Y'] = y_prov2[0]
                        del y_prov1, y_prov2
                    else:
                        y_prov = df_nodos_ct.loc[df_nodos_ct['CT_Y']>0].reset_index(drop=True)['CT_Y'][0]
                        if y_prov >= 0:
                            df_nodos_ct.loc[index, 'NUDO_Y'] = y_prov
                        else:
                            df_nodos_ct.loc[index, 'NUDO_Y'] = 0
                        del y_prov1, y_prov2, y_prov
                except:
                    df_nodos_ct.loc[index, 'NUDO_Y'] = 0
                logger.error('NODOS: Error de coordenada Y (' + str(row.NUDO_Y) + ') para el nodo ' + str(row.ID_NODO) + ' LBT_ID ' + str(row.LBT_ID))
                
                
        
        #Se modifica el DF df_nodos_ct para añadir la columna ID_NODO_LBT_ID. Se crea un nodo para cada LBT asociada al mismo
        try:
            temp=[]
            temp = df_nodos_ct['ID_NODO'].apply(str) + '_' + df_nodos_ct['LBT_ID'].astype('str')#apply(str).str.strip('.0')
            df_nodos_ct['ID_NODO_LBT_ID'] = temp
            del temp
        except:
            logger.error('NODOS: Error al calcular los nuevos IDs de los nodos, asociados con las LBT. Datos columnas "ID_NODO_LBT_ID": ' + str(len(df_nodos_ct['ID_NODO_LBT_ID'])) + '. Revisar formato datos, los IDs y los LBT deben ser enteros.')
            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 2
      
        try:      
            #Se modifica el DF df_traza_ct para añadir las columnas NODO_ORIGEN_ID_VANO_BT y NODO_DESTINO_ID_VANO_BT
            temp=[]
            temp = df_traza_ct['NODO_ORIGEN'].apply(str) + '_' + df_traza_ct['LBT_ID'].astype('str')#.apply(str).str.strip('.0')
            df_traza_ct['NODO_ORIGEN_LBT_ID'] = temp
            del temp
        except:
            logger.error('TRAZAS: Error al calcular los nuevos IDs del NODO_ORIGEN de las trazas, asociados con las LBT. Datos columna "NODO_ORIGEN_LBT_ID": ' + str(len(df_traza_ct['NODO_ORIGEN_LBT_ID'])) + '. Revisar formato datos, los IDs y los LBT deben ser enteros.')
            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 2
        try:
            temp=[]
            temp = df_traza_ct['NODO_DESTINO'].apply(str) + '_' + df_traza_ct['LBT_ID'].astype('str')#.apply(str).str.strip('.0')
            df_traza_ct['NODO_DESTINO_LBT_ID'] = temp
            del temp
        except:
            logger.error('TRAZAS: Error al calcular los nuevos IDs del NODO_DESTINO de las trazas, asociados con las LBT. Datos columna "NODO_DESTINO_LBT_ID": ' + str(len(df_traza_ct['NODO_DESTINO_LBT_ID'])) + '. Revisar formato datos, los IDs y los LBT deben ser enteros.')
            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 2
      

        
        
        #Identificar los LBT_ID diferentes que hay en la red. LBT_ID = numeración única. LBT_NOMBRE = 1,2,3...
        #Se comprueba también con el LBT_ID de la tabla de trazas. Idealmente al adjuntarlo al LBT_ID_list original y eliminar duplicados no quedaría ningún LBT_ID de trazas. Si queda la columna TRAFO y LBT_NOMBRE tendrán NAN.
        LBT_ID_list = df_nodos_ct.loc[:,['TRAFO','LBT_ID','LBT_NOMBRE']].drop_duplicates(subset=['TRAFO', 'LBT_ID', 'LBT_NOMBRE'], keep = 'first').reset_index(drop=True)
        LBT_ID_prov = df_traza_ct.loc[:,['TRAFO','LBT_ID']].drop_duplicates(subset=['LBT_ID'], keep = 'first').reset_index(drop=True)
        LBT_ID_list = LBT_ID_list.append(LBT_ID_prov, ignore_index=True).reset_index(drop=True)#.drop_duplicates(subset=['LBT_ID'], keep = 'first').reset_index(drop=True)
        #Se ordenan de forma descendente para que los posibles trafos vacíos o Nan queden abajo, y al eliminar duplicados, para un mismo LBT_ID hay varios trafos, quedarse con el primero que será TR...
        LBT_ID_list = LBT_ID_list.sort_values('TRAFO', ascending=False).drop_duplicates(subset=['LBT_ID'], keep = 'first').reset_index(drop=True)
        logger.debug('LBTs localizadas (tabla de nodos + trazas): ' + str(LBT_ID_list))
        del LBT_ID_prov
        
        
        #Se comprueba cuantas trazas tienen longitud 0. Si es más del 20% se aborta la generación del grafo porque no se conseguirán valores adecuados de pérdidas.
        # trazas_cero = len(df_traza_ct.loc[df_traza_ct.Longitud == 0])
        if len(df_traza_ct) > 0: #Hay que asegurarse de no dividir por 0.
            if trazas_cero*100/len(df_traza_ct) > 20:
                if graph_data_error <= 3:
                    graph_data_error = 3
                logger.error('TRAZAS: Se han encontrado más de un 20% de trazas con longitud = 0. Puede deberse a coordenadas incorrectas. No se puede generar un grafo aceptable.')
                logger.error('CREACIÓN DEL GRAFO ABORTADA. ERRORES INCOMPATIBLES CON UNA CORRECTA DEFINICIÓN. Graph_data_error = ' + str(graph_data_error))
                print('CREACIÓN DEL GRAFO ABORTADA. ERRORES INCOMPATIBLES CON UNA CORRECTA DEFINICIÓN. Graph_data_error = ' + str(graph_data_error))
                self.update_graph_data_error(graph_data_error)
                # return None
                # return graph_data_error, 0, 0, 0, 0, 0, 0
                df_nodos_ct=[]
                df_traza_ct=[]
                df_ct_cups_ct=[]
                df_matr_dist=[]
                LBT_ID_list=[]
                cups_agregado_CT=[]
                return graph_data_error, df_nodos_ct, df_traza_ct, df_ct_cups_ct, df_matr_dist, LBT_ID_list, cups_agregado_CT
                # return graph_data_error
            else:
                logger.debug('.CSV de trazas leído y filtrado. df_traza_ct = ' + str(len(df_traza_ct)))
        else:
            logger.debug('TRAZAS: .CSV de trazas leído y filtrado. df_traza_ct = ' + str(len(df_traza_ct)))
            logger.error('TRAZAS: Error al filtrar las trazas por el nombre del CT indicado: ' + self.Nombre_CT + '. Revisar que el nombre se corresponde con la columna "CT_NOMBRE" del .CSV o que el archivo indicado es el adecuado.')
            # df_traza_ct = df_traza_ct.append({'CT': str(self.id_ct), 'CT_NOMBRE': str(self.Nombre_CT), 'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'LBT_NOMBRE': 0}, ignore_index=True)
            # if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
            #     graph_data_error = 3
            # print('CREACIÓN DEL GRAFO ABORTADA. ERRORES INCOMPATIBLES CON UNA CORRECTA DEFINICIÓN. Graph_data_error = ' + str(graph_data_error))
            # logger.error('CREACIÓN DEL GRAFO ABORTADA. ERRORES INCOMPATIBLES CON UNA CORRECTA DEFINICIÓN. Graph_data_error = ' + str(graph_data_error))
            # self.update_graph_data_error(graph_data_error)
            # # return None
            # return graph_data_error, 0, 0, 0, 0, 0, 0
            # # return graph_data_error
        
        del df_traza
        
        
        #Se localizan los diferentes trafos de la red y los posibles errores.
        TRAFOS_LIST = LBT_ID_list.loc[:,['TRAFO']].drop_duplicates(subset=['TRAFO'], keep = 'first').reset_index(drop=True)
        logger.debug('TRAFOS localizados (tabla de nodos + trazas): ' + str(TRAFOS_LIST))
        if TRAFOS_LIST.isnull().values.any():
            logger.error('Localizadas LBTs no asignadas a trafos. Comprobar errores')
            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 2
 
        
 
        #Archivo que relaciona CUPS y del CT.
        df_ct_cups = pd.read_csv(self.archivo_ct_cups, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal=',')
        df_ct_cups_ct = df_ct_cups[(df_ct_cups['CT_NOMBRE'] == self.Nombre_CT) & (df_ct_cups['CT'] == self.id_ct)].drop_duplicates(keep = 'first').reset_index(drop=True).copy()
        #Se elimintan todos los valores NaN que pueda haber
        df_ct_cups_ct.CUPS.fillna('', inplace=True)
        df_ct_cups_ct.LBT_ID.fillna('', inplace=True)
        df_ct_cups_ct.TIPO_CONEXION.fillna('', inplace=True)
        df_ct_cups_ct.AMM_FASE.fillna('', inplace=True)
        df_ct_cups_ct.TRAFO.fillna('', inplace=True)
        
        
        df_ct_cups_ct['CUPS'] = df_ct_cups_ct['CUPS'].str.upper().str.replace(' ', '')
        df_ct_cups_ct['AMM_FASE'] = df_ct_cups_ct['AMM_FASE'].str.upper().str.replace(' ', '') #Poner en mayúsculas la columna de las fases, para evitar errores.
        df_ct_cups_ct['TRAFO'] = df_ct_cups_ct['TRAFO'].str.upper()
        
        cups_400 = []
        cups_230 = []
        
        for index,row in df_ct_cups_ct.iterrows():
            df_ct_cups_ct.loc[index, 'LBT_NOMBRE'] = str(row.LBT_NOMBRE).replace('.0', '').replace(' ', '')
            df_ct_cups_ct.loc[index, 'LBT_ID'] = str(row.LBT_ID).replace('.0', '').replace(' ', '')
            df_ct_cups_ct.loc[index, 'TIPO_CONEXION'] = str(row.TIPO_CONEXION).upper().replace('.0', '').replace(' ', '')
            df_ct_cups_ct.loc[index, 'TRAFO'] = str(row.TRAFO).replace(' ', '').replace('.','').replace(',','')
            df_ct_cups_ct.loc[index, 'QBT_TENSION'] = str(row.QBT_TENSION).replace('.0', '').replace(' ', '')
            
            try:
                if int(row.QBT_TENSION) >= 350:
                    cups_400.append(row.CUPS)
                elif int(row.QBT_TENSION) >= 200 and int(row.QBT_TENSION) < 350:
                    cups_230.append(row.CUPS)
            except:
                logger.error('Error al localizar el QBT_TENSION del CUPS ' + str(row.CUPS) + '. Si es un TRAFGISS no implica error.')
            
            try:
                if int(row.LBT_ID) > 0:
                    aa='Todo ok'
                    del aa
                else:
                    logger.error('CT-CUPS: Posible error en el LBT_ID ' + str(row.LBT_ID).replace('.0', '').replace(' ', '') + ' del CUPS ' + str(row.CUPS))
                    if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 1
            except:
                if row.CTE_GISS >= 0:
                    aa='Todo ok'
                    del aa
                else:
                    logger.error('CT-CUPS: Posible error en el LBT_ID ' + str(row.LBT_ID).replace('.0', '').replace(' ', '') + ' del CUPS ' + str(row.CUPS))
                    if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 1
       
            
        try:
            df_ct_cups_ct['CUPS_X'] = df_ct_cups_ct['CUPS_X'].astype('float')
            df_ct_cups_ct['CUPS_Y'] = df_ct_cups_ct['CUPS_Y'].astype('float')
        except:
            if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 1
            logger.error('Error al intentar asegurar que las coordenadas X, Y del archivo CT-CUPS son "float".')
            for index, row in df_ct_cups_ct.iterrows():
                try:
                    df_ct_cups_ct.loc[index, 'CUPS_X'] = float(str(row.CUPS_X).replace(',','.'))
                    df_ct_cups_ct.loc[index, 'CUPS_Y'] = float(str(row.CUPS_Y).replace(',','.'))
                except:
                    logger.error('Error en ' + str(row.CUPS) + ' con CUPS_X ' + str(row.CUPS_X) + ' o CUPS_Y ' + str(row.CUPS_Y))
                    #continue
            
        if len(df_ct_cups_ct) > 0:
            logger.debug('.CSV con la info de los CUPS leído y filtrado. df_ct_cups_ct = ' + str(len(df_ct_cups_ct)) + '. Calculando matriz de distancias entre cada CUPS y su nodo más cercano del mismo trafo.')
        else:
            logger.error('Error al filtrar los CUPS por el nombre del CT indicado: ' + self.Nombre_CT + '. Revisar que el nombre se corresponde con la columna "CT_NOMBRE" del .CSV o que el archivo indicado es el adecuado.')
            if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 3
            
        del df_ct_cups   
        
        
        #Se extraen todos los nodos diferentes del DF de trazas, para usarlos al relacionar CUPS-Nodo si es necesario
        #Se comprueba que el DF de nodos contiene todos los nodos existentes en el DF de trazas (el CT no está).
        #Se juntan nodos origen y destino de trazas y se comprueba en df_nodos.
        # df_Nodos_Trazas = []
        df_Nodos_Trazas  = df_nodos_ct.loc[:,['TRAFO', 'LBT_ID', 'ID_NODO', 'NUDO_X', 'NUDO_Y']].drop_duplicates(subset=['TRAFO', 'LBT_ID', 'ID_NODO'], keep = 'first').reset_index(drop=True).copy()
        df_temp = df_traza_ct.loc[:,['TRAFO', 'LBT_ID', 'NODO_ORIGEN', 'X_ORIGEN', 'Y_ORIGEN']].drop_duplicates(subset=['TRAFO', 'LBT_ID', 'NODO_ORIGEN'], keep = 'first').reset_index(drop=True).copy()
        df_temp.rename(columns={'NODO_ORIGEN':'ID_NODO','X_ORIGEN':'NUDO_X', 'Y_ORIGEN': 'NUDO_Y'}, inplace=True)
        df_Nodos_Trazas = df_Nodos_Trazas.append(df_temp, ignore_index=True).reset_index(drop=True)
        df_temp2 = df_traza_ct.loc[:,['TRAFO', 'LBT_ID', 'NODO_DESTINO', 'X_DESTINO', 'Y_DESTINO']].drop_duplicates(subset=['TRAFO', 'LBT_ID', 'NODO_DESTINO'], keep = 'first').reset_index(drop=True).copy()
        df_temp2.rename(columns={'NODO_DESTINO':'ID_NODO','X_DESTINO':'NUDO_X', 'Y_DESTINO': 'NUDO_Y'}, inplace=True)
        df_Nodos_Trazas = df_Nodos_Trazas.append(df_temp2, ignore_index=True).reset_index(drop=True)
        del df_temp, df_temp2
        df_Nodos_Trazas  = df_Nodos_Trazas.drop_duplicates(subset=['LBT_ID', 'ID_NODO'], keep = 'first').reset_index(drop=True).copy()
        
        if len(df_nodos_ct) < len(df_Nodos_Trazas)-3: #Dejamos un rango de error de 3 nodos, hay muchos que simplemente es 1 menos, ya que el CT no cuenta en el DF de nodos
            logger.warning('Los nodos del DF de trazas son ' + str(len(df_Nodos_Trazas)) + ', más que los del DF de nodos: ' + str(len(df_nodos_ct)))
        
        
        
        
        #Matriz de distancias
        #Creación de la matriz de distancias. Columnas: CUPS, Arqueta más cercana y distancia.
        #CUIDADO. Hay redes donde no hay nodos definidos, solo trazas.
        df_matr_dist = []
        df_matr_dist = df_ct_cups_ct[['CUPS', 'TRAFO']] #Selección de todos los CUPS del CT
        df_matr_dist = df_matr_dist.reindex(columns = df_matr_dist.columns.tolist() + ["ID_Nodo_Cercano", "TR_NODO", "Distancia"]) #Añadir las 2 columnas vacías
        
        #Se localizan los CUPS correspondientes a la cabecera del CT. Tiene, CTE_GISS > 0
        cups_agregado_CT = pd.DataFrame(columns=['CUPS', 'TRAFO', 'CUPS_X', 'CUPS_Y'])
        cups_agregado_CT = df_ct_cups_ct.loc[df_ct_cups_ct['CTE_GISS'] > 0][['CUPS','TRAFO','CUPS_X','CUPS_Y']].reset_index(drop=True)
        
        #Se comprueba si hay trazas y nodos en el grafo. Si no hay se inventa un nodo para cada trafo y se unen todos los CUPS en línea recta con ese nodo y se estiman las pérdidas.
        # grafo_solo_cups = 0 #Variable para considerar este caso concreto al agregar las curvas de carga en los nodos y calcular pérdidas con los parámetros genéricos de la librería cable.py.
        # if len(df_Nodos_Trazas) == 0:
        #     # for row in df_ct_cups_ct['LBT_ID'].drop_duplicates(keep = 'first').reset_index(drop=True):
        #     try:   
        #         for row in df_ct_cups_ct.loc[:,['LBT_NOMBRE','TRAFO','LBT_ID']].drop_duplicates(keep = 'first').reset_index(drop=True).itertuples():
        #             if row.TRAFO not in df_Nodos_Trazas.TRAFO or row.LBT_ID not in df_Nodos_Trazas.LBT_ID:
        #                 if len(row.TRAFO) > 0 and len(row.LBT_ID) > 0:
        #                     coord_X = cups_agregado_CT.sort_values('CUPS_X', ascending=False).CUPS_X[0]
        #                     coord_Y = cups_agregado_CT.sort_values('CUPS_Y', ascending=False).CUPS_Y[0]
        #                     df_traza_ct = df_traza_ct.append({'CT': str(self.id_ct), 'CT_NOMBRE': str(self.Nombre_CT), 'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'ID_VANO_BT': 0, 'NODO_ORIGEN': str(self.id_ct), 'X_ORIGEN': coord_X, 'Y_ORIGEN': coord_Y, 'NODO_DESTINO': '123', 'X_DESTINO': coord_X, 'Y_DESTINO': coord_Y, 'TIPO_UBICACION': 'AEREO', 'CABLE': '4X16_CU', 'CABLE_ORIG': '4X16_CU', 'Longitud': 0, 'NODO_ORIGEN_LBT_ID': str(self.id_ct) + '_' + str(row.LBT_ID), 'NODO_DESTINO_LBT_ID': '123_' + str(row.LBT_ID), 'LBT_NOMBRE': str(row.LBT_NOMBRE)}, ignore_index=True).reset_index(drop=True)
        #                     df_Nodos_Trazas = df_Nodos_Trazas.append({'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'ID_NODO': '123', 'NUDO_X': coord_X, 'NUDO_Y': coord_Y}, ignore_index=True).reset_index(drop=True)
        #                     LBT_ID_list = LBT_ID_list.append({'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'LBT_NOMBRE': str(row.LBT_NOMBRE)}, ignore_index=True).reset_index(drop=True)
        #                     # grafo_solo_cups = 1
        #                     if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
        #                         graph_data_error = 2
        #     except:
        #         if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
        #             graph_data_error = 3
                    
        
        #Se comprueba si hay trazas y nodos en el grafo. Si no hay se inventa un nudo y enlace para cada CUPS, directamente con el CT, de forma que se pueda resolver el grafo considerando uniones en línea recta.
        if len(df_Nodos_Trazas) == 0:
            # for row in df_ct_cups_ct['LBT_ID'].drop_duplicates(keep = 'first').reset_index(drop=True):
            try:   
                #Si tampoco  hay CUPS en la red se aborta la ejecución.
                if len(df_ct_cups_ct) > 0:
                    #Se recorre el DF de CUPS para añadir los nodos y enlaces fictícios.
                    for index, row in df_ct_cups_ct.iterrows():
                        #Los CUPS del CT no se consideran.
                        if row.CTE_GISS > 0:
                            aa = 'GIS'
                            del aa
                        else:
                            #Se intenta dar al nuevo nodo las coordenadas del CUPS, y se crea una traza entre ese nodo y el CT. Si hay errores de coordenadas se intenta primero asignar al nodo las coordenadas del CT (longitud de traza 0) y sino directamente coordenadas 0.
                            try:
                                coord_X = row.CUPS_X
                                coord_Y = row.CUPS_Y
                                coord_X_CT = cups_agregado_CT.sort_values('CUPS_X', ascending=False).CUPS_X[0]
                                coord_Y_CT = cups_agregado_CT.sort_values('CUPS_Y', ascending=False).CUPS_Y[0]
                                if coord_X > 0 and coord_Y > 0:
                                    aa = 'Todo ok'
                                    del aa
                                else:
                                    coord_X = coord_X_CT
                                    coord_Y = coord_Y_CT
                                    if coord_X > 0 and coord_Y > 0:
                                        aa = 'Todo ok'
                                        del aa
                                    else:
                                        coord_X = 0
                                        coord_Y = 0
                            except:
                                try:
                                    coord_X = cups_agregado_CT.sort_values('CUPS_X', ascending=False).CUPS_X[0]
                                    coord_Y = cups_agregado_CT.sort_values('CUPS_Y', ascending=False).CUPS_Y[0]
                                    coord_X_CT = cups_agregado_CT.sort_values('CUPS_X', ascending=False).CUPS_X[0]
                                    coord_Y_CT = cups_agregado_CT.sort_values('CUPS_Y', ascending=False).CUPS_Y[0]
                                    if coord_X > 0 and coord_Y > 0:
                                        aa = 'Todo ok'
                                        del aa
                                except:
                                    coord_X = 0
                                    coord_Y = 0
                                    coord_X_CT = 0
                                    coord_Y_CT = 0
                            #El nuevo nodo se nombra con un número aleatorio correlativo para no repetir (longitud del DF + 1)
                            nodo_dest = str(len(df_traza_ct) + 1)
                            try:
                                if float(coord_X) > 0 and float(coord_Y) > 0 and float(coord_X_CT) > 0 and float(coord_Y_CT) > 0:
                                    #Se calcula la longitud del nuevo enlace
                                    long_calc = math.sqrt((float(coord_X_CT) - float(coord_X))**2 + (float(coord_Y_CT) - float(coord_Y))**2)
                                else:
                                    long_calc = 0
                            except:
                                long_calc = 0
                            #Se añade el nuevo nodo y enlace a los DFs necesarios.
                            df_traza_ct = df_traza_ct.append({'CT': str(self.id_ct), 'CT_NOMBRE': str(self.Nombre_CT), 'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'ID_VANO_BT': nodo_dest, 'NODO_ORIGEN': str(self.id_ct), 'X_ORIGEN': coord_X_CT, 'Y_ORIGEN': coord_Y_CT, 'NODO_DESTINO': nodo_dest, 'X_DESTINO': coord_X, 'Y_DESTINO': coord_Y, 'TIPO_UBICACION': 'AEREO', 'CABLE': '4X16_CU', 'CABLE_ORIG': '4X16_CU', 'Longitud': long_calc, 'NODO_ORIGEN_LBT_ID': str(self.id_ct) + '_' + str(row.LBT_ID), 'NODO_DESTINO_LBT_ID': nodo_dest + '_' + str(row.LBT_ID), 'LBT_NOMBRE': str(row.LBT_NOMBRE)}, ignore_index=True).reset_index(drop=True)
                            df_Nodos_Trazas = df_Nodos_Trazas.append({'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'ID_NODO': nodo_dest, 'NUDO_X': coord_X, 'NUDO_Y': coord_Y}, ignore_index=True).reset_index(drop=True)
                            LBT_ID_list = LBT_ID_list.append({'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'LBT_NOMBRE': str(row.LBT_NOMBRE)}, ignore_index=True).reset_index(drop=True)
                            LBT_ID_list = LBT_ID_list.drop_duplicates(keep = 'first').reset_index(drop=True)
                            
                            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                graph_data_error = 2
                else:
                    #Si no hay CUPS se aborta la ejecución.
                    if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 3
                    self.update_graph_data_error(graph_data_error)
                    # return None
                    # return graph_data_error, 0, 0, 0, 0, 0, 0
                    df_nodos_ct=[]
                    df_traza_ct=[]
                    df_ct_cups_ct=[]
                    df_matr_dist=[]
                    LBT_ID_list=[]
                    cups_agregado_CT=[]
                    return graph_data_error, df_nodos_ct, df_traza_ct, df_ct_cups_ct, df_matr_dist, LBT_ID_list, cups_agregado_CT
                    # return graph_data_error
            except:
                #Ante cualquier error no contemplado se aborta la ejecución
                if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 3
                self.update_graph_data_error(graph_data_error)
                # return None
                # return graph_data_error, 0, 0, 0, 0, 0, 0
                df_nodos_ct=[]
                df_traza_ct=[]
                df_ct_cups_ct=[]
                df_matr_dist=[]
                LBT_ID_list=[]
                cups_agregado_CT=[]
                return graph_data_error, df_nodos_ct, df_traza_ct, df_ct_cups_ct, df_matr_dist, LBT_ID_list, cups_agregado_CT
                # return graph_data_error


        
        
        for index, row in df_ct_cups_ct.iterrows():
            if row.CTE_GISS > 0:
                # df2 = pd.DataFrame({"CUPS":[row.CUPS], "TRAFO":[row.TRAFO], "CUPS_X":[row.CUPS_X], "CUPS_Y":[row.CUPS_Y]}) 
                # cups_agregado_CT = cups_agregado_CT.append(df2, ignore_index=True).reset_index(drop=True)
                # del df2
                df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = self.id_ct
                df_matr_dist.loc[index, 'TR_NODO'] = row.TRAFO
                df_matr_dist.loc[index, 'Distancia'] = 0
            else:
                #Se comprueba si hay trazas y nodos en el grafo. Si no hay se inventa un nodo para unir todos los CUPS en línea recta con ese nodo y estimar las pérdidas.
                # if len(df_traza_ct) > 0:
                nodo_ok = 0 #Para comprobar que se encuentra un nodo para ese CUP
                error_lbt = 0 #Para detectar si hay un error en el LBT y guardar en el LOG
                distancia = 99999999999999999999 #Valor muy grande de distancia, para actualizarlo hasta encontrar el valor más pequeño. CUIDADO, puede haber errores de coordenadas que den distancias muy largas.
                for index2, row2 in df_Nodos_Trazas.iterrows():
                    distancia_prov = math.sqrt((float(row['CUPS_X']) - float(row2['NUDO_X']))**2 + (float(row['CUPS_Y']) - float(row2['NUDO_Y']))**2)
                    #Cuidado, algún CUPS no tiene asignado un LBT_ID correcto.
                    if len(LBT_ID_list.loc[LBT_ID_list.LBT_ID == row.LBT_ID]) >= 1:
                        #if distancia_prov < distancia and row.TRAFO == row2.TRAFO and row.LBT_ID == row2.LBT_ID and str(row2.ID_NODO) != str(self.id_ct):
                        if distancia_prov < distancia and row.TRAFO == row2.TRAFO and str(row2.ID_NODO) != str(self.id_ct):
                            error_lbt = 1 #Para indicar que no hay error de LBT
                            distancia = distancia_prov
                            df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = row2['ID_NODO']
                            df_matr_dist.loc[index, 'TR_NODO'] = row2['TRAFO']
                            df_matr_dist.loc[index, 'Distancia'] = distancia
                            nodo_ok = 1
                    else:
                        if distancia_prov < distancia and row.TRAFO == row2.TRAFO and str(row2.ID_NODO) != str(self.id_ct):
                            error_lbt = 0
                            distancia = distancia_prov
                            df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = row2['ID_NODO']
                            df_matr_dist.loc[index, 'TR_NODO'] = row2['TRAFO']
                            df_matr_dist.loc[index, 'Distancia'] = distancia
                            nodo_ok = 1
                if error_lbt == 0:
                    logger.error('Posible error en el LBT ' + str(row.LBT_ID) + ' del CUPS ' + str(row.CUPS))
                    if graph_data_error <= 1: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 1
                
                if nodo_ok == 0:
                    logger.error('Error al identificar el nodo correspondiente al CUP ' + str(row.CUPS) + '. No se ha encontrado ningun nodo que se corresponda con el trafo (' + str(row.TRAFO) + ').')
                    distancia = 0
                    df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = '0'
                    df_matr_dist.loc[index, 'TR_NODO'] = row.TRAFO
                    df_matr_dist.loc[index, 'Distancia'] = distancia
                    #Se comprueba si el trafo y la LBT están en LBT_ID_List
                    if len(LBT_ID_list.loc[LBT_ID_list.TRAFO == row.TRAFO]) >= 1:
                        if len(LBT_ID_list.loc[LBT_ID_list.LBT_ID == row.LBT_ID]) >= 1:
                            df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = '1'
                    else:
                        #Si el trafo no está en LBT_ID_list se comprueba si puede ser un trafo válido (más de 2 caracteres)
                        if len(row.TRAFO) >= 2:
                            LBT_ID_list = LBT_ID_list.append({'TRAFO': str(row.TRAFO), 'LBT_ID': str(row.LBT_ID), 'LBT_NOMBRE': 0}, ignore_index=True)
                            LBT_ID_list = LBT_ID_list.reset_index(drop=True)
                            df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = '1'
                                          
                    # if len(LBT_ID_list.loc[LBT_ID_list.TRAFO == row.TRAFO and LBT_ID_list.LBT_ID == row.LBT_ID]) >= 1:
                    #     LBT_ID_list = LBT_ID_list.append({'TRAFO': str(row.TRAFO), 'LBT_ID_list': str(row.LBT_ID).replace('.0','').replace(' ',''), 'LBT_NOMBRE': 0}, ignore_index=True)
                    if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 2
                #Si la distancia es demasiado larga (posible error de coordenadas), se define a un valor bajo.
                # if distancia > 50:
                #     df_matr_dist.loc[index, 'Distancia'] = 50
                
                #Se comprueba el QBT_TENSION del CUPS y se verifica que existe ese nivel de tensión en el trafo asociado.
                try:
                    if int(row.QBT_TENSION) >= 200 and int(row.QBT_TENSION) < 350:
                        if len(cups_agregado_CT.loc[cups_agregado_CT['TRAFO'] == str(row.TRAFO)]) > 0:
                            aa = 'Probando' #Variable de comprobación de que se ha obtenido un cups correcto para ese trafo y nivel de tensión
                            for lista_cups_trafo in cups_agregado_CT.loc[cups_agregado_CT['TRAFO'] == str(row.TRAFO)].reset_index(drop=True).itertuples():
                                if str(lista_cups_trafo.CUPS).find(str(row.TRAFO.replace('R','') + '1')) >= 0:
                                    aa = 'Todo ok'
                                    break
                            if aa == 'Todo ok':
                                del aa
                            else:
                                del aa
                                if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                    graph_data_error = 2
                                print('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')
                                logger.error('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')

                        else:
                            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                graph_data_error = 2
                            print('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')
                            logger.error('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')
                    
                    if int(row.QBT_TENSION) >= 350:
                        if len(cups_agregado_CT.loc[cups_agregado_CT['TRAFO'] == str(row.TRAFO)]) > 0:
                            aa = 'Probando' #Variable de comprobación de que se ha obtenido un cups correcto para ese trafo y nivel de tensión
                            for lista_cups_trafo in cups_agregado_CT.loc[cups_agregado_CT['TRAFO'] == str(row.TRAFO)].reset_index(drop=True).itertuples():
                                if str(lista_cups_trafo.CUPS).find(str(row.TRAFO.replace('R','') + '2')) >= 0:
                                    aa = 'Todo ok'
                                    break
                            if aa == 'Todo ok':
                                del aa
                            else:
                                del aa
                                if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                    graph_data_error = 2
                                print('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')
                                logger.error('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')

                        else:
                            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                                graph_data_error = 2
                            print('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')
                            logger.error('CUPS ' + str(row.CUPS) + ' con QBT_TENSION ' + str(row.QBT_TENSION) + ' y trafo ' + str(row.TRAFO) + ' no tiene asociado en el trafo ningún CUPS con ese nivel de tensión.')
                except:
                    logger.error('Error al identificar el QBT_TENSION ' + str(row.QBT_TENSION) + ' del CUPS ' + str(row.CUPS) + ' con trafo ' + str(row.TRAFO))
                    
        
        if len(df_matr_dist) > 0:
            logger.debug('Matriz de distancia calculada y guardada en el archivo: ' + self.ruta_raiz + r'Matr_Dist_' + self.Nombre_CT + '_' + str(self.id_ct) + '.csv' + ' para el CT seleccionado.')
        else:
            logger.error('Error al calcular la matriz de distancias. Revisar datos.')
            if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 3
            
        #Se comprueba que las coordenadas de los CUPS del CT son las correctas.
        for index, row in cups_agregado_CT.iterrows():
            try:
                if float(row.CUPS_X) <= 0:
                    cups_agregado_CT.loc[index, 'CUPS_X'] = df_nodos_ct.sort_values('CT_X', ascending=False).CT_X.drop_duplicates(keep = 'first').reset_index(drop=True)[0]
            except:
                try:
                    cups_agregado_CT.loc[index, 'CUPS_X'] = df_nodos_ct.sort_values('CT_X', ascending=False).CT_X.drop_duplicates(keep = 'first').reset_index(drop=True)[0]
                except:
                    logger.warning('Imposible encontrar coordenada X para el CUPS de CT ' + str(row.CUPS))
            try:
               if float(row.CUPS_Y) <= 0:
                   cups_agregado_CT.loc[index, 'CUPS_Y'] = df_nodos_ct.sort_values('CT_Y', ascending=False).CT_Y.drop_duplicates(keep = 'first').reset_index(drop=True)[0]
            except:
                try:
                    cups_agregado_CT.loc[index, 'CUPS_Y'] = df_nodos_ct.sort_values('CT_Y', ascending=False).CT_Y.drop_duplicates(keep = 'first').reset_index(drop=True)[0]
                except:
                    logger.warning('Imposible encontrar coordenada Y para el CUPS de CT ' + str(row.CUPS)) 
        #Hay que comprobar cuantos CUPS hay para cada TRAFO, si hay más de 1 hay que decidir qué valor se considera como agregado en el CT.
        # for row in cups_agregado_CT['TRAFO'].value_counts():
        for idx,row in cups_agregado_CT.groupby(['TRAFO']).count().iterrows():
            if row.CUPS > 1:
                logger.warning('Detectado más de 1 CUPS en el agregado del CT para el trafo ' + str(idx))     
        logger.debug('CUPS encontrados en la cabecera: ' + str(cups_agregado_CT))
        if len(TRAFOS_LIST) != len(cups_agregado_CT):
            logger.error('Posible error de identificación de Trafos y/o de identificación de CUPS en la cabecera. cups_agregado_CT: ' + str(cups_agregado_CT))
           
        
        return graph_data_error, df_nodos_ct, df_traza_ct, df_ct_cups_ct, df_matr_dist, LBT_ID_list, cups_agregado_CT




    def genera_grafo(self, G, df_nodos_ct, df_traza_ct, LBT_ID_list, cups_agregado_CT, dicc_colors, graph_data_error):
        """
        
        Función para generar el grafo con la topología y las trazas dadas.
        Importante: Se replican los nodos y enlaces para generar tantos subgrafos como LBTs haya definidas. Se aplicará el cálculo de pérdidas considerando que cada línea parte con un cableado independiente desde el CT. Para enlazar todo esto con el CT se crean tantos CTs virtuales como LBTs haya.
        Para trafos con varios niveles de tensión a la salida (230 o 400), se parte de definir el grafo para 400 y después se duplican las LBTs necesarias en caso de que existan CUPS conectados a 230.
        
        Parámetros
        ----------
        df_nodos_ct : DataFrame con los nodos del CT indicado (archivo Topología). Se obtiene del archivo 'archivo_nodos'.
        df_traza_ct : DataFrame con las trazas del CT indicado (archivo Trazas). Se obtiene del archivo 'archivo_trazas'.
        LBT_ID_list : Listado de líneas que parten del CT.
        cups_agregado_CT : DataFrame con los CUPS encontrados en la cabecera de la red.
        dicc_colors : Diccionario de colores para definir el atributo de color de cada nodo según el tipo.
        graph_data_error : Variable para determinar la viabilidad del grafo en función de la calidad de los datos que lo definen.
            
        
        Retorno
        -------
        G : Grafo definido. 
        graph_data_error : Valor actualizado.
            Atributos de los nodos: ID_NODO_LBT_ID, TR, P_R_0, Q_R_0, P_S_0, Q_S_0, P_T_0, Q_T_0, Tipo_Nodo, pos(NUDO_X, NUDO_Y), color_nodo, QBT_TENSION.
            Atributos de los enlaces: NODO_ORIGEN_LBT_ID, NODO_DESTINO_LBT_ID, TR, Long, P_R_Linea, Q_R_Linea, P_S_Linea, Q_S_Linea, P_T_Linea, Q_T_Linea, CABLE, QBT_TENSION.
        """
        logger = logging.getLogger('genera_grafo')
        #Primero se genera un grafo que contiene todos los subgrafos por separado en función de los LBT_ID que haya.
        #Se unen todos los subgrafos en un CT 'ficticio'.
        
        #Al ser grafo NO dirigido, al agregar nodos y enlaces, si se agrega varias veces el mismo nodo o el mismo enlace no se repiten, se mantiene solo un único nodo o enlace
        #En el caso de los enlaces tampoco se repiten aunque sea en sentido contrario.
        # for index, row in df_nodos_ct.iterrows():
        #     G.add_node(row['ID_NODO_LBT_ID'], P_R = 0, Q_R = 0, P_S = 0, Q_S = 0, P_T = 0, Q_T = 0, Tipo_Nodo = row['TIPO_NODO'], pos = (float(str(row['NUDO_X']).replace(',','.')), float(str(row['NUDO_Y']).replace(',','.'))), color_nodo = 'blue')
        
        #Se añaden los atributos al nodo id_ct y un nodo virtual por cada Trafo de la red.
        for index, row in cups_agregado_CT.iterrows():
            try:
                id_ct_coord_x = row.CUPS_X
            except:
                id_ct_coord_x = 0
            try:
                id_ct_coord_y = row.CUPS_Y
            except:
                id_ct_coord_y = 0
                
            G.add_node(str(self.id_ct), TR='CT', P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', QBT_TENSION=400)
            G.add_node(str(self.id_ct) + '_' + str(row.TRAFO), TR=str(row.TRAFO), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=400)
            if (str(self.id_ct), str(self.id_ct) + '_' + str(row.TRAFO),0) not in G.edges:
                G.add_edge(str(self.id_ct), str(self.id_ct) + '_' + str(row.TRAFO), TR=str(row.TRAFO), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=400)
            #Se añaden los niveles de tensión existentes:
            QBT_tension = row.TRAFO.replace('R','')
            if row.CUPS.find(QBT_tension + '1') >= 0:
                tension_tr = 230
            elif row.CUPS.find(QBT_tension + '2') >= 0:
                tension_tr = 400
            else:
                logger.error('Error al encontrar el nivel de tensión del CUPS ' + str(row.CUPS) + '. Trafo ' + str(row.TRAFO))
                tension_tr = 0
            G.add_node(str(row.TRAFO) + '_' + str(tension_tr), TR=str(row.TRAFO), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=tension_tr)
            if (str(self.id_ct) + '_' + str(row.TRAFO), str(row.TRAFO) + '_' + str(tension_tr), 0) not in G.edges:
                G.add_edge(str(self.id_ct) + '_' + str(row.TRAFO), str(row.TRAFO) + '_' + str(tension_tr), TR=str(row.TRAFO), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=tension_tr)
            
            
        #Si no hay CUPS de agregado en el CT no se agregan los nodos correspondientes, por lo que hay que añadir al menos el CT y los trafos.
        if len(cups_agregado_CT) == 0:    
            prov = df_traza_ct.TRAFO.drop_duplicates(keep='first').reset_index(drop=True)
            prov = prov.append(df_nodos_ct.TRAFO.drop_duplicates(keep='first').reset_index(drop=True)).drop_duplicates(keep='first').reset_index(drop=True)
            # id_ct_coord_x = df_nodos_ct.CT_X.drop_duplicates(keep='first').reset_index(drop=True)
            id_ct_coord_x = df_nodos_ct[df_nodos_ct.CT_X>0].CT_X.drop_duplicates(keep='first').reset_index(drop=True)[0]
            # id_ct_coord_y = df_nodos_ct.CT_Y.drop_duplicates(keep='first').reset_index(drop=True)
            id_ct_coord_y = df_nodos_ct[df_nodos_ct.CT_Y>0].CT_Y.drop_duplicates(keep='first').reset_index(drop=True)[0]
            
            #Se agrega el CT
            G.add_node(str(self.id_ct), TR='CT', P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', QBT_TENSION=400)
            
            #Se agregan los trafos
            for row in prov:
                G.add_node(str(self.id_ct) + '_' + str(row), TR=str(row), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=400)
                if (str(self.id_ct), str(self.id_ct) + '_' + str(row),0) not in G.edges:
                    G.add_edge(str(self.id_ct), str(self.id_ct) + '_' + str(row), TR=str(row), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=400)
                #Se añaden los niveles de tensión existentes. En este caso solo de 400V, porque no se sabe si hay CUPS en 230.
                tension_tr = 400
                G.add_node(str(row) + '_' + str(tension_tr), TR=str(row), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=tension_tr)
                if (str(self.id_ct) + '_' + str(row), str(row) + '_' + str(tension_tr), 0) not in G.edges:
                    G.add_edge(str(self.id_ct) + '_' + str(row), str(row) + '_' + str(tension_tr), TR=str(row), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=tension_tr)
         
            del prov
        
        
        #Se añade el CT_TR y se crean los enlaces virtuales entre el ID_CT_TR y los correspondientes ID_CT_LBT_ID  
        for i in range(0,len(LBT_ID_list)):  
            try:
                id_ct_coord_x = df_nodos_ct.sort_values('CT_X', ascending=False).reset_index(drop=True).CT_X[0]
                id_ct_coord_y = df_nodos_ct.sort_values('CT_Y', ascending=False).reset_index(drop=True).CT_Y[0]
            except:
                id_ct_coord_x = cups_agregado_CT.sort_values('CUPS_X', ascending=False).reset_index(drop=True).CUPS_X[0]
                id_ct_coord_y = cups_agregado_CT.sort_values('CUPS_Y', ascending=False).reset_index(drop=True).CUPS_Y[0]
            #Por si acaso no se han agregado el CT y loc CT_TR se comprueba si existen los nodos y los enlaces.
            if str(self.id_ct) not in G.nodes():
                G.add_node(str(self.id_ct), TR='CT', P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', QBT_TENSION=400)
            
            if str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]) not in G.nodes():
                G.add_node(str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]), TR=str(LBT_ID_list['TRAFO'][i]), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=400)
            
            if (str(self.id_ct), str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]),0) not in G.edges:
                G.add_edge(str(self.id_ct), str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]), TR=str(LBT_ID_list['TRAFO'][i]), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=400)
            
            #Se añaden los niveles de tensión existentes. En este caso solo de 400V, porque no se sabe si hay CUPS en 230.
            tension_tr = 400
            if str(LBT_ID_list['TRAFO'][i] + '_' + str(tension_tr)) not in G.nodes:
                G.add_node(str(LBT_ID_list['TRAFO'][i]) + '_' + str(tension_tr), TR=str(LBT_ID_list['TRAFO'][i]), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=tension_tr)
                if (str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]), str(LBT_ID_list['TRAFO'][i]) + '_' + str(tension_tr), 0) not in G.edges:
                    G.add_edge(str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]), str(LBT_ID_list['TRAFO'][i]) + '_' + str(tension_tr), TR=str(LBT_ID_list['TRAFO'][i]), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=tension_tr)
         
            
            # G.add_edge(self.id_ct, str(self.id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0])
            # G.add_edge(str(self.id_ct) + '_' + str(LBT_ID_list['TRAFO'][i]), str(self.id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]), TR=str(LBT_ID_list['TRAFO'][i]), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0])
            G.add_edge(str(LBT_ID_list['TRAFO'][i]) + '_' + str(tension_tr), str(self.id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]), TR=str(LBT_ID_list['TRAFO'][i]), Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=df_traza_ct['CABLE'][0], QBT_TENSION=tension_tr)
            
            #Se añaden también los atributos del nodo que se acaba de crear con idct_lbt
            G.add_node(str(self.id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]), TR=str(LBT_ID_list['TRAFO'][i]), P_R_0=0, Q_R_0=0, P_S_0=0, Q_S_0=0, P_T_0=0, Q_T_0=0, Tipo_Nodo='CT_Virtual', pos=(float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.'))), color_nodo='red', N_ant = 1, QBT_TENSION=400)
            
            
            
        
        
        #Se añaden las diferentes trazas definidas.
        for index, row in df_traza_ct.iterrows():
            #Se añaden los atributos a los dos nodos. Si no se encuentran atributos en el DF de nodos se asignan los de las trazas.
            if row['NODO_ORIGEN_LBT_ID'] not in G.nodes():
                try:
                    tipo_nodo_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO_LBT_ID'] == row['NODO_ORIGEN_LBT_ID']]['TIPO_NODO'].reset_index(drop=True)[0] #Número de la línea
                except:
                    tipo_nodo_prov = 'NODO_DESCONOCIDO'
                    logger.error('Nodo ' + row['NODO_ORIGEN_LBT_ID'] + ', error de tipo de nodo. Asignado: ' + tipo_nodo_prov)
                    #continue
                try:
                    color_nodo_graph = str(dicc_colors.get(tipo_nodo_prov))
                    if color_nodo_graph == 'None':
                        color_nodo_graph = 'white'
                except:
                    color_nodo_graph = 'white'
                    #continue
                    
                try:
                    trafo = str(row.TRAFO)
                except:
                    logger.error('Error al buscar el trafo al que pertenece el NODO_ORIGEN ' + str(row['NODO_ORIGEN_LBT_ID']))
                    #continue
                    
                try:
                    # nodo_coord_x = df_nodos_ct.loc[df_nodos_ct['ID_NODO_LBT_ID'] == row['NODO_ORIGEN_LBT_ID']]['NUDO_X'].reset_index(drop=True)[0]
                    nodo_coord_x = row.X_ORIGEN
                    # nodo_coord_y = df_nodos_ct.loc[df_nodos_ct['ID_NODO_LBT_ID'] == row['NODO_ORIGEN_LBT_ID']]['NUDO_Y'].reset_index(drop=True)[0]
                    nodo_coord_y = row.Y_ORIGEN
                    G.add_node(row['NODO_ORIGEN_LBT_ID'], TR=trafo, P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = tipo_nodo_prov, pos = (float(str(nodo_coord_x).replace(',','.')), float(str(nodo_coord_y).replace(',','.'))), color_nodo = str(color_nodo_graph), N_ant = 0, N_suc = 0, QBT_TENSION=400)
                except:
                    nodo_coord_x = 0
                    nodo_coord_y = 0
                    G.add_node(row['NODO_ORIGEN_LBT_ID'], TR=trafo, P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = tipo_nodo_prov, pos = (float(str(nodo_coord_x).replace(',','.')), float(str(nodo_coord_y).replace(',','.'))), color_nodo = str(color_nodo_graph), N_ant = 0, N_suc = 0, QBT_TENSION=400)
                    logger.error('Error al buscar las coordenadas de traza para el NODO_ORIGEN_LBT_ID' + str(row['NODO_ORIGEN_LBT_ID']))
                    #continue
                
            if row['NODO_DESTINO_LBT_ID'] not in G.nodes():
                try:
                    tipo_nodo_prov = df_nodos_ct.loc[df_nodos_ct['ID_NODO_LBT_ID'] == row['NODO_DESTINO_LBT_ID']]['TIPO_NODO'].reset_index(drop=True)[0] #Número de la línea
                except:
                    tipo_nodo_prov = 'NODO_DESCONOCIDO'
                    logger.error('Nodo ' + row['NODO_DESTINO_LBT_ID'] + ', error de tipo de nodo. Asignado: ' + tipo_nodo_prov)
                    #continue
                try:
                    color_nodo_graph = str(dicc_colors.get(tipo_nodo_prov))
                    if color_nodo_graph == 'None':
                        color_nodo_graph = 'white'
                except:
                    color_nodo_graph = 'white'
                    #continue
                
                try:
                    trafo = str(row.TRAFO)
                except:
                    logger.error('Error al buscar el trafo al que pertenece el NODO_DESTINO_LBT_ID ' + str(row['NODO_DESTINO_LBT_ID']))
                    #continue
                try:   
                    # nodo_coord_x = df_nodos_ct.loc[df_nodos_ct['ID_NODO_LBT_ID'] == row['NODO_DESTINO_LBT_ID']]['NUDO_X'].reset_index(drop=True)[0]
                    nodo_coord_x =  row.X_DESTINO
                    # nodo_coord_y = df_nodos_ct.loc[df_nodos_ct['ID_NODO_LBT_ID'] == row['NODO_DESTINO_LBT_ID']]['NUDO_Y'].reset_index(drop=True)[0] 
                    nodo_coord_y =  row.Y_DESTINO
                    G.add_node(row['NODO_DESTINO_LBT_ID'], TR=trafo, P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = tipo_nodo_prov, pos = (float(str(nodo_coord_x).replace(',','.')), float(str(nodo_coord_y).replace(',','.'))), color_nodo = str(color_nodo_graph), N_ant = 0, N_suc = 0, QBT_TENSION=400)
                except:
                    nodo_coord_x = 0
                    nodo_coord_y = 0
                    G.add_node(row['NODO_DESTINO_LBT_ID'], TR=trafo, P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = tipo_nodo_prov, pos = (float(str(nodo_coord_x).replace(',','.')), float(str(nodo_coord_y).replace(',','.'))), color_nodo = str(color_nodo_graph), N_ant = 0, N_suc = 0, QBT_TENSION=400)
                    logger.error('Error al buscar las coordenadas de traza para el NODO_DESTINO_LBT_ID ' + str(row['NODO_DESTINO_LBT_ID']))
                    #continue
                
            #Se añaden los enlaces y los atributos
            #Se comprueba si el enlace ya se añadió antes, si es así serán otras ramificaciones. Se cambia el ID de los existentes y se añade uno más
            if (row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID']) not in G.edges():
                trafo = str(row.TRAFO)
                G.add_edge(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'], 0, ID_traza = 0, TR=trafo, Long=row['Longitud'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=row['CABLE'], QBT_TENSION=400)#, ID_repeat=0) #ID_repeat. 0: solo hay un enlace. >=1: más de 1 enlace, y número del enlace.
                #En los nodos origen y destino se crea el atributo de número de antecesores y de sucesores, para comprobar posibles errores futuros
                if (G.nodes[row['NODO_ORIGEN_LBT_ID']]['Tipo_Nodo'] != 'CT') and (G.nodes[row['NODO_ORIGEN_LBT_ID']]['Tipo_Nodo'] != 'CT_Virtual'):
                    G.nodes[row['NODO_ORIGEN_LBT_ID']]['N_suc'] += 1
                if (G.nodes[row['NODO_DESTINO_LBT_ID']]['Tipo_Nodo'] != 'CT') and (G.nodes[row['NODO_DESTINO_LBT_ID']]['Tipo_Nodo'] != 'CT_Virtual'):
                    G.nodes[row['NODO_DESTINO_LBT_ID']]['N_ant'] += 1
            else:
                #Se listan todos los enlaces del nodo origen, después se enumeran las posiciones donde se repite el enlace de interés y se calcula el número de repeticiones
                # N_enlaces = len([i for i,x in enumerate(list(G.edges(row['NODO_ORIGEN_LBT_ID']))) if x==(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])])
                #La expresión anterior no sirve porque puede haber errores y tener el mismo enlace pero cambiar nodo origen por nodo destino.
                N_enlaces = 0
                for i in range(0,1000000):
                    try:
                        G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'], i)]
                        N_enlaces += 1
                    except:
                        break
                    
                #Se añade el nuevo enlace
                trafo = str(row.TRAFO)
                G.add_edge(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'], N_enlaces, ID_traza = N_enlaces, TR=trafo, Long=row['Longitud'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=row['CABLE'], QBT_TENSION=400)#, ID_repeat=0) #ID_repeat. 0: solo hay un enlace. >=1: más de 1 enlace, y número del enlace.
                #En los nodos origen y destino se crea el atributo de número de antecesores y de sucesores, para comprobar posibles errores futuros
                #Cuidado con no añadirselo al CT o a los CTs virtuales
                if (G.nodes[row['NODO_ORIGEN_LBT_ID']]['Tipo_Nodo'] != 'CT') and (G.nodes[row['NODO_ORIGEN_LBT_ID']]['Tipo_Nodo'] != 'CT_Virtual'):
                    G.nodes[row['NODO_ORIGEN_LBT_ID']]['N_suc'] += 1
                    # G.nodes[row['NODO_ORIGEN_LBT_ID']]['P_R_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_ORIGEN_LBT_ID']]['Q_R_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_ORIGEN_LBT_ID']]['P_S_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_ORIGEN_LBT_ID']]['Q_S_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_ORIGEN_LBT_ID']]['P_T_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_ORIGEN_LBT_ID']]['Q_T_' + str(N_enlaces)] = 0
                if (G.nodes[row['NODO_DESTINO_LBT_ID']]['Tipo_Nodo'] != 'CT') and (G.nodes[row['NODO_DESTINO_LBT_ID']]['Tipo_Nodo'] != 'CT_Virtual'):
                    G.nodes[row['NODO_DESTINO_LBT_ID']]['N_ant'] += 1
                    # G.nodes[row['NODO_DESTINO_LBT_ID']]['P_R_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_DESTINO_LBT_ID']]['Q_R_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_DESTINO_LBT_ID']]['P_S_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_DESTINO_LBT_ID']]['Q_S_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_DESTINO_LBT_ID']]['P_T_' + str(N_enlaces)] = 0
                    # G.nodes[row['NODO_DESTINO_LBT_ID']]['Q_T_' + str(N_enlaces)] = 0
         
        for index, row in df_nodos_ct.iterrows():
            if row.ID_NODO_LBT_ID not in G.nodes:
                try:
                    tipo_nodo_prov = row['TIPO_NODO']
                except:
                    tipo_nodo_prov = 'NODO_DESCONOCIDO'
                    logger.error('Nodo ' + row['ID_NODO_LBT_ID'] + ', error de tipo de nodo. Asignado: ' + tipo_nodo_prov)
                    #continue
                try:
                    color_nodo_graph = str(dicc_colors.get(tipo_nodo_prov))
                    if color_nodo_graph == 'None':
                        color_nodo_graph = 'white'
                except:
                    color_nodo_graph = 'white'
                    #continue
                
                try:
                    trafo = str(row.TRAFO)
                except:
                    logger.error('Error al buscar el trafo al que pertenece el ID_NODO_LBT_ID ' + str(row['ID_NODO_LBT_ID']))
                    #continue
                try:   
                    nodo_coord_x = row['NUDO_X']
                    nodo_coord_y = row['NUDO_Y']
                    G.add_node(row['ID_NODO_LBT_ID'], TR=trafo, P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = tipo_nodo_prov, pos = (float(str(nodo_coord_x).replace(',','.')), float(str(nodo_coord_y).replace(',','.'))), color_nodo = str(color_nodo_graph), N_ant = 0, N_suc = 0)
                except:
                    nodo_coord_x = 0
                    nodo_coord_y = 0
                    G.add_node(row['ID_NODO_LBT_ID'], TR=trafo, P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = tipo_nodo_prov, pos = (float(str(nodo_coord_x).replace(',','.')), float(str(nodo_coord_y).replace(',','.'))), color_nodo = str(color_nodo_graph), N_ant = 0, N_suc = 0)
                    logger.error('Error al buscar las coordenadas de traza para el NODO_DESTINO_LBT_ID ' + str(row['NODO_DESTINO_LBT_ID']))
                    #continue
                logger.warning('Posible error. El nodo ' + str(row.ID_NODO_LBT_ID) + ' no estaba en el grafo al añadir todas las trazas. Añadido sin conexión.')
   
                    
        logger.info('Grafo original: ' + str(len(G.nodes)) +' nodos y ' + str(len(G.edges)) + ' trazas.')
        
        ###Detección de posibles errores en el grafo. Se comprueban enlaces y se añaden los que puedan faltar por error  
        #Primero se comprueba que todos los nodos tienen un antecesor (N_ant=0). Si no es así se enlazan directamente con el CT_Virtual correspondiente si se comprueba que no tienen otra ruta de acceso.
        cont_enlaces_nuevos = 0
        for nodo, data in G.nodes(data=True, default = 0):
            try:
                ruta=list(nx.shortest_path(G,str(self.id_ct), nodo))
                len_ruta = len(ruta)
            except:
                len_ruta = 0
                logger.error('Error de descripción de archivos detectado. No hay ruta entre ' + str(self.id_ct) + ' y ' + str(nodo))
            
            #Si len_ruta es mayor que 0 significa que aunque no tenga antecesores, ese nodo tiene un camino para llegar hasta él y no es necesario crear el enlace
            if (G.nodes[nodo]['Tipo_Nodo'] != 'CT') and (G.nodes[nodo]['Tipo_Nodo'] != 'CT_Virtual') and (G.nodes[nodo]['N_ant'] == 0) and (len_ruta == 0):
                #Se une directamente con el CT_LBTID. Si no existe, con un CT_LBTID cualquiera
                try:
                    nodo_origen = str(self.id_ct) + '_' + str(nodo.split('_')[1])
                    #La longitud calcula en línea recta entre el CT y el nodo.
                    longitud = math.sqrt((float(G.nodes[nodo]['pos'][0]) - float(G.nodes[nodo_origen]['pos'][0]))**2 + (float(G.nodes[nodo]['pos'][1]) - float(G.nodes[nodo_origen]['pos'][1]))**2)
                    try:
                        #Se intenta coger el mismo tipo de cable que el de el primer enlace que dependa de ese nodo. Sino se coge uno al azar de la red.
                        tipo_cable = G.edges[list(G.edges(nodo))[0][0], list(G.edges(nodo))[0][1], 0]['CABLE']
                    except:
                        tipo_cable = df_traza_ct.CABLE[0] #df_traza_ct.loc[df_traza_ct['NODO_ORIGEN_LBT_ID'] == nodo_origen]['CABLE'].reset_index(drop=True)[0]
                except:
                    nodo_origen = str(self.id_ct) + '_' + str(LBT_ID_list.loc[LBT_ID_list.TRAFO == data['TR']]['LBT_ID'].reset_index(drop=True)[0]) #LBT_ID_list.LBT_ID[0]
                    try:
                        longitud = math.sqrt((float(G.nodes[nodo]['pos'][0]) - float(G.nodes[nodo_origen]['pos'][0]))**2 + (float(G.nodes[nodo]['pos'][1]) - float(G.nodes[nodo_origen]['pos'][1]))**2)
                        if longitud > 100:
                            longitud = 100
                    except:
                        longitud = 0
                    try:
                        #Se intenta coger el mismo tipo de cable que el de el primer enlace que dependa de ese nodo. Sino se coge uno al azar de la red.
                        tipo_cable = G.edges[list(G.edges(nodo))[0][0], list(G.edges(nodo))[0][1], 0]['CABLE']
                    except:
                        tipo_cable = df_traza_ct.CABLE[0] #df_traza_ct.loc[df_traza_ct['NODO_ORIGEN_LBT_ID'] == nodo_origen]['CABLE'].reset_index(drop=True)[0]
                        #continue
                    #continue
                G.add_edge(nodo_origen, nodo, 0, ID_traza = 0, TR=str(G.nodes[nodo]['TR']), Long=longitud, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=tipo_cable, QBT_TENSION=400)#, ID_repeat=0) #ID_repeat. 0: solo hay un enlace. >=1: más de 1 enlace, y número del enlace.
                #Se añade el enlace al DF de trazas.
                df_traza_ct.loc[len(df_traza_ct)] = [str(self.id_ct), self.Nombre_CT, G.nodes[nodo]['TR'], str(nodo.split('_')[1]), '0', nodo_origen.split('_')[0], float(G.nodes[nodo_origen]['pos'][0]), float(G.nodes[nodo_origen]['pos'][1]), nodo.split('_')[0], float(G.nodes[nodo]['pos'][0]), float(G.nodes[nodo]['pos'][1]), '', tipo_cable, tipo_cable, longitud, nodo_origen, nodo]
                if (G.nodes[nodo]['Tipo_Nodo'] != 'CT') and (G.nodes[nodo]['Tipo_Nodo'] != 'CT_Virtual'):
                    G.nodes[nodo]['N_ant'] += 1
                cont_enlaces_nuevos += 1
                logger.error('Se ha detectado que no existe enlace previo al nodo ' + str(nodo) + '. Se ha creado un enlace con ' + str(nodo_origen) + '.')
                
                    
                    
                 
        ########
        ## COMPROBAR QUE EXISTE UN CAMINO ENTRE EL CT y cada nodo.
        ########
        for nodo,data in G.nodes(data=True, default = 0):
            try:
                # ruta=list(nx.shortest_path(G,id_ct, nodo))
                ruta=nx.shortest_path(G,str(self.id_ct), nodo)
    #            print(ruta)
            except:
                logger.error('Error de descripción de archivos detectado. No hay ruta entre ' + str(self.id_ct) + ' y ' + str(nodo) + '. Creado un enlace directo con el trafo TR_400.')
                try:
                    nodo_origen = str(self.id_ct) + '_' + str(nodo.split('_')[1])
                    #La longitud calcula en línea recta entre el CT y el nodo.
                    longitud = math.sqrt((float(G.nodes[nodo]['pos'][0]) - float(G.nodes[nodo_origen]['pos'][0]))**2 + (float(G.nodes[nodo]['pos'][1]) - float(G.nodes[nodo_origen]['pos'][1]))**2)
                    try:
                        #Se intenta coger el mismo tipo de cable que el de el primer enlace que dependa de ese nodo. Sino se coge uno al azar de la red.
                        tipo_cable = G.edges[list(G.edges(nodo))[0][0], list(G.edges(nodo))[0][1], 0]['CABLE']
                    except:
                        tipo_cable = df_traza_ct.CABLE[0] #df_traza_ct.loc[df_traza_ct['NODO_ORIGEN_LBT_ID'] == nodo_origen]['CABLE'].reset_index(drop=True)[0]
                except:
                    nodo_origen = str(self.id_ct) + '_' + str(LBT_ID_list.loc[LBT_ID_list.TRAFO == data['TR']]['LBT_ID'].reset_index(drop=True)[0]) #LBT_ID_list.LBT_ID[0]
                    try:
                        longitud = math.sqrt((float(G.nodes[nodo]['pos'][0]) - float(G.nodes[nodo_origen]['pos'][0]))**2 + (float(G.nodes[nodo]['pos'][1]) - float(G.nodes[nodo_origen]['pos'][1]))**2)
                    except:
                        longitud = 0
                    try:
                        #Se intenta coger el mismo tipo de cable que el de el primer enlace que dependa de ese nodo. Sino se coge uno al azar de la red.
                        tipo_cable = G.edges[list(G.edges(nodo))[0][0], list(G.edges(nodo))[0][1], 0]['CABLE']
                    except:
                        tipo_cable = df_traza_ct.CABLE[0] #df_traza_ct.loc[df_traza_ct['NODO_ORIGEN_LBT_ID'] == nodo_origen]['CABLE'].reset_index(drop=True)[0]
                        #continue
                    #continue
                G.add_edge(nodo_origen, nodo, 0, ID_traza = 0, TR=str(G.nodes[nodo]['TR']), Long=longitud, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=tipo_cable, QBT_TENSION=400)#, ID_repeat=0) #ID_repeat. 0: solo hay un enlace. >=1: más de 1 enlace, y número del enlace.
                #Se añade el enlace al DF de trazas.
                df_traza_ct.loc[len(df_traza_ct)] = [str(self.id_ct), self.Nombre_CT, G.nodes[nodo]['TR'], str(nodo.split('_')[1]), '0', nodo_origen.split('_')[0], float(G.nodes[nodo_origen]['pos'][0]), float(G.nodes[nodo_origen]['pos'][1]), nodo.split('_')[0], float(G.nodes[nodo]['pos'][0]), float(G.nodes[nodo]['pos'][1]), '', tipo_cable, tipo_cable, longitud, nodo_origen, nodo]
                if (G.nodes[nodo]['Tipo_Nodo'] != 'CT') and (G.nodes[nodo]['Tipo_Nodo'] != 'CT_Virtual'):
                    G.nodes[nodo]['N_ant'] += 1
                cont_enlaces_nuevos += 1
                
                #continue
            
            #Es necesario tener definidos estos atributos para detectar los ciclos y eliminar nodos adecuadamente.
            G.nodes[nodo]['Enlaces_orig'] = len(list(np.unique(list(G.edges(nodo)))))-1
            G.nodes[nodo]['Enlaces_iter'] = len(list(np.unique(list(G.edges(nodo)))))-1
        
        if cont_enlaces_nuevos > 0:
            logger.error('Se han detectado y creado ' + str(cont_enlaces_nuevos) + ' enlaces que no existian.')
            if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 2
        logger.info('Grafo generado tras revisar enlaces que faltan: ' + str(len(G.nodes)) +' nodos y ' + str(len(G.edges)) + ' trazas.')
        
        
        ########
        ## COMPROBAR QUE NO EXISTEN BUCLES. SI EXISTEN SE ELIMINA UN ENLACE DE UN NODO QUE NO SEA BIFURCACIÓN.
        ########
        contr_lazos = 0
        iter_lazos = 0 #Se contabiliza el número de iteraciones del while. Si sobrepasa de un valor se sale del lazo y se da un error.
        
        while contr_lazos == 0 and iter_lazos <= 20:
            iter_lazos += 1
            contr_lazos = 1
            list_cycle = [] #Se inicializa por si acaso
            list_cycle2 = [] #Se inicializa por si acaso
            bucle_found = 0  
            #Método 1 de localización de lazos (rápido pero poco fiable).
            try:
                #Este primer intento soluciona algunos lazos, pero puede identificar bucles que simplemente intercambian nodo origen y destino. Por eso se comprueba que devuelve más de 3 nodos que formna el lazo.
                #Es una instrucción muy rápida pero que puede devolver errores, por eso para ciertos grafos es necesaria una segunda comprobación con el siguiente método.
                #Se busca si hay un lazo en el grafo. Esta instrucción devuelve (nodo1, nodo2, key enlace, dirección)
                list_cycle = list(nx.find_cycle(G, source=str(self.id_ct), orientation="ignore"))
                # bucle_found = 0            
                if len(list_cycle) >= 3:
                    bucle_found = 1
                    if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 2
                    #Se rompe uno de los enlaces:
                    for j in range(0,1000000):
                        try:
                            #Se comprueba si hay enlace j
                            G.edges[(list_cycle[0][0], list_cycle[0][1], j)]
                            #Si le hay, se elimina el enlace
                            G.remove_edge(list_cycle[0][0], list_cycle[0][1], key=j)
                            # contr_lazos = 1
                            bucle_found = 0
                            print('Se deshace el enlace ' + str(list_cycle[0][0]) + '-' + str(list_cycle[0][1]) + '-' + str(j))
                            logger.warning('Se deshace el enlace ' + str(list_cycle[0][0]) + '-' + str(list_cycle[0][1]) + '-' + str(j))
                            #Se actualiza el valor de los atributos en ambos nodos
                            G.nodes[list_cycle[0][0]]['Enlaces_orig'] = len(list(np.unique(list(G.edges(list_cycle[0][0])))))-1
                            G.nodes[list_cycle[0][0]]['Enlaces_iter'] = len(list(np.unique(list(G.edges(list_cycle[0][0])))))-1
                            G.nodes[list_cycle[0][1]]['Enlaces_orig'] = len(list(np.unique(list(G.edges(list_cycle[0][1])))))-1
                            G.nodes[list_cycle[0][1]]['Enlaces_iter'] = len(list(np.unique(list(G.edges(list_cycle[0][1])))))-1
                        
                        except:
                            break

                    #Se vuelve a comprobar si existe otro posible bucle después de analizar el primero
                    try:
                        list_cycle2 = list(nx.find_cycle(G, source=str(self.id_ct), orientation="ignore"))
                        if len(list_cycle2) >= 3:
                            contr_lazos = 0                            
                    except:
                        list_cycle2 = []
                        pass
                    # if bucle_found == 1:
                    #     print('Encontrado un bucle no resuelto.')
                    #     graph_data_error = 3
                    #     contr_lazos = 0
            except:
                #Si no se encuentra un ciclo se sale del while.
                contr_lazos = 1
                
                
            #Función para encontrar todos los ciclos existentes en un grafo. Código adaptado de https://gist.github.com/joe-jordan/6548029
            def find_all_cycles(G):
                nodes=[str(self.id_ct)]
                # extra variables for cycle detection:
                cycle_stack = []
                output_cycles = set()
                
                def get_hashable_cycle(cycle):
                    """cycle as a tuple in a deterministic order."""
                    m = min(cycle)
                    mi = cycle.index(m)
                    mi_plus_1 = mi + 1 if mi < len(cycle) - 1 else 0
                    if cycle[mi-1] > cycle[mi_plus_1]:
                        result = cycle[mi:] + cycle[:mi]
                    else:
                        result = list(reversed(cycle[:mi_plus_1])) + list(reversed(cycle[mi_plus_1:]))
                    return tuple(result)
                
                for start in nodes:
                    if start in cycle_stack:
                        continue
                    cycle_stack.append(start)
                    
                    stack = [(start,iter(G[start]))]
                    while stack:
                        parent,children = stack[-1]
                        try:
                            child = next(children)
                            
                            if child not in cycle_stack:
                                cycle_stack.append(child)
                                stack.append((child,iter(G[child])))
                            else:
                                i = cycle_stack.index(child)
                                if i < len(cycle_stack) - 2: 
                                  output_cycles.add(get_hashable_cycle(cycle_stack[i:]))
                            
                        except StopIteration:
                            stack.pop()
                            cycle_stack.pop()
                
                list_bucles = ([list(i) for i in output_cycles])
                list_bucles = sorted(list_bucles, key=lambda x: len(x))
                return list_bucles
            
            list_bucles = []
            list_bucles = find_all_cycles(G)
    
            #Método 2 de localización de lazos (más lento. Se ejecuta si el primer método es ambiguo.)
            if len(list_cycle) == 2 or len(list_cycle2) == 2 or bucle_found == 1 or len(list_bucles) > 0:
                while len(list_bucles) > 0:
                    if graph_data_error <= 2: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                        graph_data_error = 2
                    #list_bucles tienes los bucles ordenados de menor a mayor número de enlaces. Se coge siempre el primer bucle, se deshace y se vuelven a comprobar los bucles.
                    #Al deshacer un bucle se toma siempre el último enlace, por se el más alejado del CT.
                    #if len(list_bucles[0]) >= 3:
                    nodo_ini = list_bucles[0][len(list_bucles[0])-2]
                    nodo_fin = list_bucles[0][len(list_bucles[0])-1]
                    for j in range(0,1000000):
                        try:
                            #Se comprueba si hay enlace j
                            G.edges[(nodo_ini, nodo_fin, j)]
                            #Si le hay, se elimina el enlace
                            G.remove_edge(nodo_ini, nodo_fin, key=j)
                            # contr_lazos = 1
                            bucle_found = 0
                            print('Se deshace el enlace ' + str(nodo_ini) + '-' + str(nodo_fin) + '-' + str(j))
                            logger.warning('Se deshace el enlace ' + str(nodo_ini) + '-' + str(nodo_fin) + '-' + str(j))
                            #Se actualiza el valor de los atributos en ambos nodos
                            G.nodes[nodo_ini]['Enlaces_orig'] = len(list(np.unique(list(G.edges(nodo_ini)))))-1
                            G.nodes[nodo_ini]['Enlaces_iter'] = len(list(np.unique(list(G.edges(nodo_ini)))))-1
                            G.nodes[nodo_fin]['Enlaces_orig'] = len(list(np.unique(list(G.edges(nodo_fin)))))-1
                            G.nodes[nodo_fin]['Enlaces_iter'] = len(list(np.unique(list(G.edges(nodo_fin)))))-1
                        except:
                            break
                    
                    list_bucles = []
                    list_bucles = find_all_cycles(G)
                                
                            
            if bucle_found == 1:
                print('Encontrado un bucle no resuelto.')
                if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                    graph_data_error = 3
                contr_lazos = 0

                    
        if iter_lazos >= 20:
            logger.error('Sobrepasado el valor máximo de lazos definido. Posible error en la definición del código y lazos insalvables.')
            if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 3
       
        return G, graph_data_error
    
    
    
    def add_cups_grafo(self, G, df_ct_cups_ct, df_matr_dist, id_ct, LBT_ID_list, dicc_colors):
        logger = logging.getLogger('add_cups_grafo')
        #Se recorre el DF con los CUPS y se asocia cada CUP al nodo correspondiente.
        for index, row in df_ct_cups_ct.iterrows():
            if row['CTE_GISS'] > 0 or row.CUPS.find('GISS')>=0:
                #Se añade al grafo el CUP del agregado en el CT.
                cup_lbt_id = '0'
                trafo_cup = str(row['TRAFO'])
                cup_amm_fase = 'GISS'
                cup_tipo_conexion = 'GISS'
                arqueta_cup_lbt_id = str(self.id_ct) + '_' + str(row.TRAFO)
                longitud = 0
                if str(row.CUPS) not in G.nodes:
                    G.add_edges_from( [(arqueta_cup_lbt_id, str(row.CUPS))])
                
                #Primero se definen todas las posibles fases
                G.nodes[str(row.CUPS)]['P_R_0'] = 0
                G.nodes[str(row.CUPS)]['Q_R_0'] = 0
                G.nodes[str(row.CUPS)]['P_S_0'] = 0
                G.nodes[str(row.CUPS)]['Q_S_0'] = 0
                G.nodes[str(row.CUPS)]['P_T_0'] = 0
                G.nodes[str(row.CUPS)]['Q_T_0'] = 0
                G.nodes[str(row.CUPS)]['LBT_ID'] = str(cup_lbt_id)
                G.nodes[str(row.CUPS)]['TIPO_CONEXION'] = cup_tipo_conexion
                G.nodes[str(row.CUPS)]['AMM_FASE'] = cup_amm_fase
                G.nodes[str(row.CUPS)]['Tipo_Nodo'] = 'CUPS_TR'
                G.nodes[str(row.CUPS)]['TR'] = trafo_cup
                
                #Se añade el nivel de tensión del trafo
                QBT_tension = str(G.nodes[str(row.CUPS)]['TR']).replace('R','')
                if row.CUPS.find(QBT_tension + '1') >= 0:
                    tension_tr = 230
                elif row.CUPS.find(QBT_tension + '2') >= 0:
                    tension_tr = 400
                else:
                    logger.error('Error al encontrar el nivel de tensión del CUPS ' + str(row.CUPS) + '. Trafo ' + str(row.TRAFO))
                    tension_tr = 0
                G.nodes[str(row.CUPS)]['QBT_TENSION'] = tension_tr
                            
                        
                id_ct_coord_x = row['CUPS_X']
                id_ct_coord_y = row['CUPS_Y']
                G.nodes[str(row.CUPS)]['pos'] = (float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.')))
                G.nodes[str(row.CUPS)]['color_nodo'] = str(dicc_colors.get('CUPS_TR'))
                G.nodes[str(row.CUPS)]['N_ant'] = 1
                
                #Se definen los atributos de la línea que une el nodo con el CUPS
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Long'] = float(longitud)
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['P_R_Linea'] = 0
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Q_R_Linea'] = 0
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['P_S_Linea'] = 0
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Q_S_Linea'] = 0
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['P_T_Linea'] = 0
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Q_T_Linea'] = 0
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['TR'] = trafo_cup
                G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['QBT_TENSION'] = tension_tr

            else:
                cup_lbt_nombre = row['LBT_NOMBRE'] #Número de la línea
                try:
                    cup_lbt_id = row['LBT_ID']
                    cup_lbt_id = str(pd.to_numeric(cup_lbt_id, downcast='integer')).replace('.0', '')
                except:
                    cup_lbt_id = str(cup_lbt_id).replace('.0', '')
                
                qbt_tension = row['QBT_TENSION'] #Salida del trafo a la que está conectado el CUPS. T12 = 400V, T11=230V.
                    
                trafo_cup = str(row['TRAFO'])
                cup_tipo_conexion = row['TIPO_CONEXION'] #Monofásico / trifásico
                cup_pot_max = row['POT_MAX']  # kW
                cup_tipo_actividad = row['TIPO_ACTIVIDAD'] #Residencial, Industria, Servicios
                
                cup_amm_fase = row['AMM_FASE'] #Fase de conexión. R, S, T
                if cup_amm_fase == 'R' or cup_amm_fase == 'S' or cup_amm_fase == 'T':
                    aa = 'todo ok'
                    del aa
                else:
                    df_ct_cups_ct.loc[index, 'AMM_FASE'] = 'R'
                    logger.error('Error al buscar la fase del CUP ' + str(row.CUPS) + ' y tipo de conexión ' + str(cup_tipo_conexion) + '. Asignada fase R.')
                
                arqueta = df_matr_dist.loc[df_matr_dist['CUPS'] == row.CUPS]['ID_Nodo_Cercano'].reset_index(drop=True)[0]
                arqueta_cup_lbt_id = str(arqueta) + '_' + str(cup_lbt_id)
                
                if str(arqueta).replace('.0','') == '1':
                    arqueta_cup_lbt_id = str(self.id_ct) + '_' + str(row.LBT_ID)
              
                if str(arqueta).replace('.0','') == '0':
                    aa = 'error nodo'
                    del aa
                # elif str(arqueta).replace('.0','') == '1':
                    
                else:
                    ########
                    #COMPROBAR SI arqueta_cup_lbt_id EXISTE EN EL GRAFO
                    #Si no existe, se asocia el CUP al NODO_LBT_ID que exista. (Se asume el error pero se garantiza que el CUP queda conectado al grafo)
                    ########
                    if arqueta_cup_lbt_id not in G.nodes:
                        for lbt_row in LBT_ID_list.itertuples():
                            arqueta_temp = str(arqueta) + '_' + str(lbt_row.LBT_ID)
                            if arqueta_temp in G.nodes and lbt_row.TRAFO == trafo_cup:
                                logger.warning('Nodo con ID_NODO_LBT_ID ' + arqueta_cup_lbt_id + ' no encontrado. Se asocia el CUP_LBT_ID ' +  str(row.CUPS) + '_' + str(cup_lbt_id) + ' al ID_NODO_LBT_ID ' + arqueta_temp)
                                print('Nodo con ID_NODO_LBT_ID ' + arqueta_cup_lbt_id + ' no encontrado. Se asocia el CUP_LBT_ID ' +  str(row.CUPS) + '_' + str(cup_lbt_id) + ' al ID_NODO_LBT_ID ' + arqueta_temp)
                                arqueta_cup_lbt_id = arqueta_temp
                                break
                            
                    longitud = df_matr_dist.loc[df_matr_dist['CUPS'] == row.CUPS]['Distancia'].reset_index(drop=True)[0]
                    #Si la distancia es demasiado larga (posible error de coordenadas), se define a un valor bajo.
                    if longitud > 50:
                        longitud = 50
                        
                        
                    #Se crea una función que calcula Enlaces_orig y Enlaces_iter para los nodos de 230. Es necesaria porque sino con el método original se cuentan los CUPS que puedan estar añadidos a un nodo intermedio.
                    def enlaces_iter_orig(nodo):
                        cont_enlaces_nodo = 0
                        for enlace_i in list(np.unique(list(G.edges(str(nodo))))):
                            if G.nodes[enlace_i]['Tipo_Nodo'] != 'CUPS' and G.nodes[enlace_i]['Tipo_Nodo'] != 'CUPS_TR':
                                cont_enlaces_nodo += 1
                        return cont_enlaces_nodo-1 #-1 para no contar el propio nodo.
                    
                        
                     #Comprobamos si se trata de un CUPS con nivel de tensión de 230 V. Si es así se duplica la ruta entre el trafo y el CUPS, indicando en cada nodo _230.
                    if int(qbt_tension) >= 200 and int(qbt_tension) < 350:           
                        qbt_tension = 230 #Se define a 230, ya que puede tener valores de 220 o similares
                        if G.nodes[arqueta_cup_lbt_id]['Tipo_Nodo'] == 'CT_Virtual' and arqueta_cup_lbt_id + '_230' not in G.nodes:
                            G.add_node(arqueta_cup_lbt_id + '_230', TR = G.nodes[arqueta_cup_lbt_id]['TR'], P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = G.nodes[arqueta_cup_lbt_id]['Tipo_Nodo'], pos = G.nodes[arqueta_cup_lbt_id]['pos'], color_nodo = G.nodes[arqueta_cup_lbt_id]['color_nodo'], N_ant = 0, N_suc = 0, QBT_TENSION=qbt_tension)
                            if G.nodes[arqueta_cup_lbt_id]['TR'] + '_230' not in G.nodes:
                                G.add_node(G.nodes[arqueta_cup_lbt_id]['TR'] + '_230', TR = G.nodes[arqueta_cup_lbt_id]['TR'], P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = G.nodes[arqueta_cup_lbt_id]['Tipo_Nodo'], pos = G.nodes[arqueta_cup_lbt_id]['pos'], color_nodo = G.nodes[arqueta_cup_lbt_id]['color_nodo'], N_ant = 0, N_suc = 0, QBT_TENSION=qbt_tension)
                                # G.add_edge(G.nodes[arqueta_cup_lbt_id]['TR'] + '_230', arqueta_cup_lbt_id + '_230', 0, ID_traza = 0, TR=G.nodes[arqueta_cup_lbt_id]['TR'], Long=0, P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(ruta[len(ruta)-1]), str(arqueta_cup_lbt_id),0]['CABLE'])
                                
                            ruta=list(nx.shortest_path(G,str(self.id_ct),arqueta_cup_lbt_id))
                            ruta.remove(arqueta_cup_lbt_id)
                            if (str(ruta[len(ruta)-1]).replace('400','230'), str(arqueta_cup_lbt_id) + '_230') not in G.edges:
                                G.add_edge(str(ruta[len(ruta)-1]).replace('400','230'), str(arqueta_cup_lbt_id) + '_230', 0, ID_traza = 0, TR=G.edges[str(ruta[len(ruta)-1]), str(arqueta_cup_lbt_id),0]['TR'], Long=G.edges[str(ruta[len(ruta)-1]), str(arqueta_cup_lbt_id),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(ruta[len(ruta)-1]), str(arqueta_cup_lbt_id),0]['CABLE'], QBT_TENSION=qbt_tension)
                            if (str(ruta[len(ruta)-2]), str(ruta[len(ruta)-1]).replace('400','230')) not in G.edges:
                                G.add_edge(str(ruta[len(ruta)-2]), str(ruta[len(ruta)-1]).replace('400','230'), 0, ID_traza = 0, TR=G.edges[str(ruta[len(ruta)-2]), str(ruta[len(ruta)-1]), 0]['TR'], Long=G.edges[str(ruta[len(ruta)-2]), str(ruta[len(ruta)-1]),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(ruta[len(ruta)-2]), str(ruta[len(ruta)-1]),0]['CABLE'], QBT_TENSION=qbt_tension)
                                
                            cont_enlaces_nodo = enlaces_iter_orig(str(ruta[len(ruta)-1]).replace('400','230'))
                            G.nodes[str(ruta[len(ruta)-1]).replace('400','230')]['Enlaces_orig'] = cont_enlaces_nodo
                            G.nodes[str(ruta[len(ruta)-1]).replace('400','230')]['Enlaces_iter'] = cont_enlaces_nodo
                            cont_enlaces_nodo = enlaces_iter_orig(str(arqueta_cup_lbt_id) + '_230')
                            G.nodes[str(arqueta_cup_lbt_id) + '_230']['Enlaces_orig'] = cont_enlaces_nodo
                            G.nodes[str(arqueta_cup_lbt_id) + '_230']['Enlaces_iter'] = cont_enlaces_nodo
                            cont_enlaces_nodo = enlaces_iter_orig(str(ruta[len(ruta)-2]))
                            G.nodes[str(ruta[len(ruta)-2])]['Enlaces_orig'] = cont_enlaces_nodo
                            G.nodes[str(ruta[len(ruta)-2])]['Enlaces_iter'] = cont_enlaces_nodo
                            
                            # G.nodes[str(ruta[len(ruta)-1]).replace('400','230')]['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(ruta[len(ruta)-1]).replace('400','230'))))))-1
                            # G.nodes[str(ruta[len(ruta)-1]).replace('400','230')]['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(ruta[len(ruta)-1]).replace('400','230'))))))-1
                            # G.nodes[str(arqueta_cup_lbt_id) + '_230']['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(arqueta_cup_lbt_id) + '_230')))))-1
                            # G.nodes[str(arqueta_cup_lbt_id) + '_230']['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(arqueta_cup_lbt_id) + '_230')))))-1
                            
                            # arqueta_cup_lbt_id = arqueta_cup_lbt_id + '_230'
                            
                        elif arqueta_cup_lbt_id + '_230' not in G.nodes and G.nodes[arqueta_cup_lbt_id]['Tipo_Nodo'] != 'CT_Virtual':
                            G.add_node(arqueta_cup_lbt_id + '_230', TR = G.nodes[arqueta_cup_lbt_id]['TR'], P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = G.nodes[arqueta_cup_lbt_id]['Tipo_Nodo'], pos = G.nodes[arqueta_cup_lbt_id]['pos'], color_nodo = G.nodes[arqueta_cup_lbt_id]['color_nodo'], N_ant = 0, N_suc = 0, QBT_TENSION=qbt_tension)
                            ruta=list(nx.shortest_path(G,str(self.id_ct),arqueta_cup_lbt_id))
                            ruta.remove(arqueta_cup_lbt_id)
                            nodo_old = arqueta_cup_lbt_id
                            localiza_ct = 0 #Identifica cuando se ha entrado en un CT_Virtual para crear un único enlace entre la LBT_ID y el trafo.
                            for row2 in reversed(ruta):
                                #Se comprueba si row2 no está ya definido previamente.
                                if row2 + '_230' not in G.nodes and row2 != str(G.nodes[arqueta_cup_lbt_id]['TR']) + '_400':
                                    if localiza_ct == 1:
                                        # G.add_edge(str(row2), str(nodo_old) + '_230', 0, ID_traza = 0, TR=G.edges[str(row2), str(nodo_old),0]['TR'], Long=G.edges[str(row2), str(nodo_old),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(row2), str(nodo_old),0]['CABLE'])
                                        
                                        G.add_node(str(row2).replace('400','230'), TR = G.nodes[str(row2)]['TR'], P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = G.nodes[str(row2)]['Tipo_Nodo'], pos = G.nodes[str(row2)]['pos'], color_nodo = G.nodes[str(row2)]['color_nodo'], N_ant = 0, N_suc = 0, QBT_TENSION=qbt_tension)
                                        G.add_edge(str(row2).replace('400','230'), str(nodo_old) + '_230', 0, ID_traza = 0, TR=G.edges[str(row2), str(nodo_old),0]['TR'], Long=G.edges[str(row2), str(nodo_old),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(row2), str(nodo_old),0]['CABLE'], QBT_TENSION=qbt_tension)
                                        G.add_edge(str(self.id_ct) + '_' + str(row2).replace('_400',''), str(row2).replace('400','230'), 0, ID_traza = 0, TR=G.edges[str(row2), str(nodo_old),0]['TR'], Long=G.edges[str(row2), str(nodo_old),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(row2), str(nodo_old),0]['CABLE'], QBT_TENSION=qbt_tension)
                                        cont_enlaces_nodo = enlaces_iter_orig(str(row2).replace('400','230'))
                                        G.nodes[str(row2).replace('400','230')]['Enlaces_orig'] = cont_enlaces_nodo
                                        G.nodes[str(row2).replace('400','230')]['Enlaces_iter'] = cont_enlaces_nodo
                                        cont_enlaces_nodo = enlaces_iter_orig(str(self.id_ct) + '_' + str(row2).replace('_400',''))
                                        G.nodes[str(self.id_ct) + '_' + str(row2).replace('_400','')]['Enlaces_orig'] = cont_enlaces_nodo
                                        G.nodes[str(self.id_ct) + '_' + str(row2).replace('_400','')]['Enlaces_iter'] = cont_enlaces_nodo
                                        
                                        # G.nodes[str(row2).replace('400','230')]['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(row2).replace('400','230'))))))-1
                                        # G.nodes[str(row2).replace('400','230')]['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(row2).replace('400','230'))))))-1
                                        # G.nodes[str(self.id_ct) + '_' + str(row2).replace('_400','')]['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(self.id_ct) + '_' + str(row2).replace('_400',''))))))-1
                                        # G.nodes[str(self.id_ct) + '_' + str(row2).replace('_400','')]['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(self.id_ct) + '_' + str(row2).replace('_400',''))))))-1
                                        
                                        
                                    else:
                                        G.add_node(str(row2) + '_230', TR = G.nodes[str(row2)]['TR'], P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = G.nodes[str(row2)]['Tipo_Nodo'], pos = G.nodes[str(row2)]['pos'], color_nodo = G.nodes[str(row2)]['color_nodo'], N_ant = 0, N_suc = 0, QBT_TENSION=qbt_tension)
                                        G.add_edge(str(row2) + '_230', str(nodo_old) + '_230', 0, ID_traza = 0, TR=G.edges[str(row2), str(nodo_old),0]['TR'], Long=G.edges[str(row2), str(nodo_old),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(row2), str(nodo_old),0]['CABLE'], QBT_TENSION=qbt_tension)
                                    cont_enlaces_nodo = enlaces_iter_orig(nodo_old + '_230')
                                    G.nodes[nodo_old + '_230']['Enlaces_orig'] = cont_enlaces_nodo
                                    G.nodes[nodo_old + '_230']['Enlaces_iter'] = cont_enlaces_nodo                                    
                                    
                                    # G.nodes[nodo_old + '_230']['Enlaces_orig'] = len(list(np.unique(list(G.edges(nodo_old + '_230')))))-1
                                    # G.nodes[nodo_old + '_230']['Enlaces_iter'] = len(list(np.unique(list(G.edges(nodo_old + '_230')))))-1
                                    if G.nodes[row2]['Tipo_Nodo'] == 'CT_Virtual':
                                        if localiza_ct == 1:
                                            cont_enlaces_nodo = enlaces_iter_orig(str(row2).replace('400','230'))
                                            G.nodes[str(row2).replace('400','230')]['Enlaces_orig'] = cont_enlaces_nodo
                                            G.nodes[str(row2).replace('400','230')]['Enlaces_iter'] = cont_enlaces_nodo
                                            
                                            # G.nodes[str(row2).replace('400','230')]['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(row2).replace('400','230'))))))-1
                                            # G.nodes[str(row2).replace('400','230')]['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(row2).replace('400','230'))))))-1
                                        # if localiza_ct == 1:
                                            break
                                        localiza_ct = 1
                                    nodo_old = row2
                                else:
                                    if localiza_ct == 1:
                                        if str(row2).replace('400','230') not in G.nodes:
                                            G.add_node(str(row2).replace('400','230'), TR = G.nodes[str(row2)]['TR'], P_R_0 = 0, Q_R_0 = 0, P_S_0 = 0, Q_S_0 = 0, P_T_0 = 0, Q_T_0 = 0, Tipo_Nodo = G.nodes[str(row2)]['Tipo_Nodo'], pos = G.nodes[str(row2)]['pos'], color_nodo = G.nodes[str(row2)]['color_nodo'], N_ant = 0, N_suc = 0, QBT_TENSION=qbt_tension)
                                        G.add_edge(str(row2).replace('400','230'), str(nodo_old) + '_230', 0, ID_traza = 0, TR=G.edges[str(row2), str(nodo_old),0]['TR'], Long=G.edges[str(row2), str(nodo_old),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(row2), str(nodo_old),0]['CABLE'], QBT_TENSION=qbt_tension)
                                        cont_enlaces_nodo = enlaces_iter_orig(nodo_old + '_230')
                                        G.nodes[nodo_old + '_230']['Enlaces_orig'] = cont_enlaces_nodo
                                        G.nodes[nodo_old + '_230']['Enlaces_iter'] = cont_enlaces_nodo
                                        cont_enlaces_nodo = enlaces_iter_orig(str(row2).replace('400','230'))
                                        G.nodes[str(row2).replace('400','230')]['Enlaces_orig'] = cont_enlaces_nodo
                                        G.nodes[str(row2).replace('400','230')]['Enlaces_iter'] = cont_enlaces_nodo
                                        
                                        # G.nodes[nodo_old + '_230']['Enlaces_orig'] = len(list(np.unique(list(G.edges(nodo_old + '_230')))))-1
                                        # G.nodes[nodo_old + '_230']['Enlaces_iter'] = len(list(np.unique(list(G.edges(nodo_old + '_230')))))-1
                                        # G.nodes[str(row2).replace('400','230')]['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(row2.replace('400','230')))))))-1
                                        # G.nodes[str(row2).replace('400','230')]['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(row2.replace('400','230')))))))-1
                                        break
                                    else:
                                        G.add_edge(str(row2) + '_230', str(nodo_old) + '_230', 0, ID_traza = 0, TR=G.edges[str(row2), str(nodo_old),0]['TR'], Long=G.edges[str(row2), str(nodo_old),0]['Long'], P_R_Linea=0, Q_R_Linea=0, P_S_Linea=0, Q_S_Linea=0, P_T_Linea=0, Q_T_Linea=0, CABLE=G.edges[str(row2), str(nodo_old),0]['CABLE'], QBT_TENSION=qbt_tension)
                                        cont_enlaces_nodo = enlaces_iter_orig(nodo_old + '_230')
                                        G.nodes[nodo_old + '_230']['Enlaces_orig'] = cont_enlaces_nodo
                                        G.nodes[nodo_old + '_230']['Enlaces_iter'] = cont_enlaces_nodo
                                        cont_enlaces_nodo = enlaces_iter_orig(str(row2) + '_230')
                                        G.nodes[str(row2) + '_230']['Enlaces_orig'] = cont_enlaces_nodo
                                        G.nodes[str(row2) + '_230']['Enlaces_iter'] = cont_enlaces_nodo
                                        
                                        # G.nodes[nodo_old + '_230']['Enlaces_orig'] = len(list(np.unique(list(G.edges(nodo_old + '_230')))))-1
                                        # G.nodes[nodo_old + '_230']['Enlaces_iter'] = len(list(np.unique(list(G.edges(nodo_old + '_230')))))-1
                                        # G.nodes[str(row2) + '_230']['Enlaces_orig'] = len(list(np.unique(list(G.edges(str(row2) + '_230')))))-1
                                        # G.nodes[str(row2) + '_230']['Enlaces_iter'] = len(list(np.unique(list(G.edges(str(row2) + '_230')))))-1
                                        break                                    
            
                        arqueta_cup_lbt_id = arqueta_cup_lbt_id + '_230' #Se define ahora para poder utilizar antes los datos de la arqueta original.
                        
                    else:
                        qbt_tension = 400 #Se unifica a 400 porque puede tomar valor de 380, 440 o similar.
                        
                        
                    #Una vez comprobados los enlaces del nodo se añade el CUP
                    if str(row.CUPS) not in G.nodes:
                        G.add_edges_from( [(arqueta_cup_lbt_id, str(row.CUPS))])
                    
                    #Primero se definen todas las posibles fases
                    G.nodes[str(row.CUPS)]['P_R_0'] = 0
                    G.nodes[str(row.CUPS)]['Q_R_0'] = 0
                    G.nodes[str(row.CUPS)]['P_S_0'] = 0
                    G.nodes[str(row.CUPS)]['Q_S_0'] = 0
                    G.nodes[str(row.CUPS)]['P_T_0'] = 0
                    G.nodes[str(row.CUPS)]['Q_T_0'] = 0
                    G.nodes[str(row.CUPS)]['LBT_ID'] = str(cup_lbt_id)
                    G.nodes[str(row.CUPS)]['TIPO_CONEXION'] = cup_tipo_conexion
                    G.nodes[str(row.CUPS)]['AMM_FASE'] = cup_amm_fase
                    G.nodes[str(row.CUPS)]['Tipo_Nodo'] = 'CUPS'
                    G.nodes[str(row.CUPS)]['TR'] = trafo_cup
                    G.nodes[str(row.CUPS)]['QBT_TENSION'] = qbt_tension
                    
                            
                    id_ct_coord_x = row['CUPS_X']
                    id_ct_coord_y = row['CUPS_Y']
                    G.nodes[str(row.CUPS)]['pos'] = (float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.')))
                    G.nodes[str(row.CUPS)]['color_nodo'] = str(dicc_colors.get('CUPS'))
                    G.nodes[str(row.CUPS)]['N_ant'] = 1
                    
                    #Se definen los atributos de la línea que une el nodo con el CUPS
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Long'] = float(longitud)
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['P_R_Linea'] = 0
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Q_R_Linea'] = 0
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['P_S_Linea'] = 0
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Q_S_Linea'] = 0
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['P_T_Linea'] = 0
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['Q_T_Linea'] = 0
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['TR'] = trafo_cup         
                    G.edges[(arqueta_cup_lbt_id, str(row.CUPS),0)]['QBT_TENSION'] = qbt_tension
        return G
    
    
    
    def get_cch_cups(self, fecha, ruta_cch, cups_grafo):
        """
        
        Función para leer los archivos .csv de una carpeta, filtrarlos para el CT de análisis, obtener las curvas de carga de los clientes y de los CUPS del CT para un mes completo.
        Importante: Se extrae el mes completo de la fecha dada para agilizar la lectura de datos. Al cambiar de mes se vuelve a llamar a esta función.
        
        Parámetros
        ----------
        fecha : Fecha para extraer los datos. Se devolverán los datos de ese mes completo.
        ruta_cch : Carpeta con los archivos de curvas de carga. Se tienen todos los relativos a clientes y CTs.
        cups_grafo : DataFrame con los CUPS que componen el grafo de análisis.
            
        
        Retorno
        -------
        df_cch : DataFrame con las curvas de carga de los clientes.
        df_cch_AE_giss : DataFrame con las curvas de carga de energía suministrada por los CUPS de medida en el CT.
        df_cch_AS_giss : DataFrame con las curvas de carga de energía recibida de los clientes (autoconsumo) por los CUPS de medida en el CT.
        """
        logger = logging.getLogger('get_cch_cups')
        df_cch = pd.DataFrame(columns=['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25', 'FLAG_H01', 'FLAG_H02', 'FLAG_H03', 'FLAG_H04', 'FLAG_H05', 'FLAG_H06', 'FLAG_H07', 'FLAG_H08', 'FLAG_H09', 'FLAG_H10', 'FLAG_H11', 'FLAG_H12', 'FLAG_H13', 'FLAG_H14', 'FLAG_H15', 'FLAG_H16', 'FLAG_H17', 'FLAG_H18', 'FLAG_H19', 'FLAG_H20', 'FLAG_H21', 'FLAG_H22', 'FLAG_H23', 'FLAG_H24', 'FLAG_H25'])
        # df_cch2 = pd.DataFrame(columns=['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25', 'FLAG_H01', 'FLAG_H02', 'FLAG_H03', 'FLAG_H04', 'FLAG_H05', 'FLAG_H06', 'FLAG_H07', 'FLAG_H08', 'FLAG_H09', 'FLAG_H10', 'FLAG_H11', 'FLAG_H12', 'FLAG_H13', 'FLAG_H14', 'FLAG_H15', 'FLAG_H16', 'FLAG_H17', 'FLAG_H18', 'FLAG_H19', 'FLAG_H20', 'FLAG_H21', 'FLAG_H22', 'FLAG_H23', 'FLAG_H24', 'FLAG_H25'])
        df_cch_AE_giss = pd.DataFrame(columns=['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25', 'FLAG_H01', 'FLAG_H02', 'FLAG_H03', 'FLAG_H04', 'FLAG_H05', 'FLAG_H06', 'FLAG_H07', 'FLAG_H08', 'FLAG_H09', 'FLAG_H10', 'FLAG_H11', 'FLAG_H12', 'FLAG_H13', 'FLAG_H14', 'FLAG_H15', 'FLAG_H16', 'FLAG_H17', 'FLAG_H18', 'FLAG_H19', 'FLAG_H20', 'FLAG_H21', 'FLAG_H22', 'FLAG_H23', 'FLAG_H24', 'FLAG_H25'])
        df_cch_AS_giss = pd.DataFrame(columns=['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25', 'FLAG_H01', 'FLAG_H02', 'FLAG_H03', 'FLAG_H04', 'FLAG_H05', 'FLAG_H06', 'FLAG_H07', 'FLAG_H08', 'FLAG_H09', 'FLAG_H10', 'FLAG_H11', 'FLAG_H12', 'FLAG_H13', 'FLAG_H14', 'FLAG_H15', 'FLAG_H16', 'FLAG_H17', 'FLAG_H18', 'FLAG_H19', 'FLAG_H20', 'FLAG_H21', 'FLAG_H22', 'FLAG_H23', 'FLAG_H24', 'FLAG_H25'])
        #Listar contenido carpeta curvas de carga
        contenido = os.listdir(ruta_cch)
        for i in contenido:
            palabra_clave = 'CAPTADA' #Palafra que identifica a los clientes. Para el CT es GISS
            if i.find(palabra_clave) >= 0:
                df_temp = []
                #Ahora se comprueba si el archivo es el que se corresponde con la fecha indicada
                if i.find(str(fecha)[0:6]) >= 0:
                    try:
                        archivo_cch = ruta_cch + i
                        iter_csv = pd.read_csv (archivo_cch, iterator=True, chunksize=1000, encoding='Latin9', header=0, sep=';', quotechar='\"', error_bad_lines = False)
                        for df_temp in iter_csv:
                            #df_temp = df_temp.loc[df_temp['FECHA'] == fecha]
                            df_temp['CUPS'] = df_temp['CUPS'].str.upper().replace(' ', '')
                            # df_cch = df_cch.append(df_temp[df_temp.CUPS.isin(list(df_ct_cups_ct.CUPS))], ignore_index=True).reset_index(drop=True)
                            df_cch = df_cch.append(df_temp[df_temp.CUPS.isin(cups_grafo)], ignore_index=True).reset_index(drop=True)
                        del iter_csv, df_temp
                    except:
                        logger.error('Error al leer el archivo con las curvas de carga de los clientes: ' + archivo_cch + '. Ejecución abortada.')
                        raise
            
            #Se busca también el archivo correspondiente a las medidas a la salida del CT
            palabra_clave_2 = 'AE_GISS'
            if i.find(palabra_clave_2) >= 0:
                df_temp = []
                if i.find(str(fecha)[0:6]) >= 0:
                    #Lectura del archivo con las medidas a la salida del CT:
                    try:
                        archivo_cch_giss = ruta_cch + i
                        
                        df_temp = pd.read_csv(archivo_cch_giss, encoding='Latin9', header=0, sep=';', quotechar='\"', error_bad_lines = False)
                        df_cch_AE_giss = df_temp.loc[(df_temp['CODIGO_LVC'].str.find(str(self.id_ct)) >= 0)].reset_index(drop=True)
                        del df_temp
                        # df_cch_AE_giss = pd.read_csv(archivo_cch_giss, encoding='Latin9', header=0, sep=';', quotechar='\"', error_bad_lines = False)
                    except:
                        logger.error('Error al leer el archivo con las curvas de carga del CT: ' + archivo_cch_giss + '. Ejecución abortada.')
                        raise
                        
            #Se busca también el archivo correspondiente a las medidas a la salida del CT
            palabra_clave_3 = 'AS_GISS'
            if i.find(palabra_clave_3) >= 0:
                df_temp = []
                if i.find(str(fecha)[0:6]) >= 0:
                    #Lectura del archivo con las medidas a la salida del CT:
                    try:
                        archivo_cch_giss = ruta_cch + i
                        
                        df_temp = pd.read_csv(archivo_cch_giss, encoding='Latin9', header=0, sep=';', quotechar='\"', error_bad_lines = False)
                        df_cch_AS_giss = df_temp.loc[(df_temp['CODIGO_LVC'].str.find(str(self.id_ct)) >= 0)].reset_index(drop=True)
                        del df_temp
                        # df_cch_AE_giss = pd.read_csv(archivo_cch_giss, encoding='Latin9', header=0, sep=';', quotechar='\"', error_bad_lines = False)
                    except:
                        logger.error('Error al leer el archivo con las curvas de carga del CT: ' + archivo_cch_giss + '. Ejecución abortada.')
                        raise
            
        
        #Se eliminan duplicados al terminar. Se han localizado CUPS trifásicos que están el archivo de TF4 y en TF5 con las mismas fechas.
        df_cch = df_cch.drop_duplicates(keep = 'first').reset_index(drop=True)
        df_cch_AE_giss = df_cch_AE_giss.drop_duplicates(keep = 'first').reset_index(drop=True)
    
        return df_cch, df_cch_AE_giss, df_cch_AS_giss
    

    
    def add_cch_grafo(self, G, colum_hora, df_AE_fecha, df_AS_fecha):#, cups_agregado_CT, id_ct):
        """
        
        Función para añadir al grafo los valores de potencia de las curvas de carga tanto de clientes como de medido en el CT para una hora concreta, para el posterior cálculo de pérdidas.
        Importante: Si el CUPS es monofásico se agrega en el nodo al que está conectado la potencia en la fase correspondiente. Si es trifásico se asume equilibrado y se añade 1/3 de la potencia en cada fase.
        
        Parámetros
        ----------
        G : Grafo del CT.
        colum_hora : Columna del DF de curvas de carga con el valor de la hora a analizar [VALOR_H01, VALOR_H02, ... , VALOR_H24].
        df_AE_fecha : DataFrame con la curva de carga de energía activa consumida (AE) para todos los clientes del CT para la fecha indicada.
        df_AS_fecha : DataFrame con la curva de carga de energía activa suministrada a la red (AS, autoconsumo vertido) para todos los clientes del CT para la fecha indicada.
                    
        
        Retorno
        -------
        G : Grafo con los valores de potencia en los nodos con CUPS de clientes conectados.
        """
        logger = logging.getLogger('add_cch_grafo')
        
        #Antes de añadir las curvas de carga poner a 0 los parámetros de potencia de todos los nodos.
        for nodo1, nodo2, keys, data in G.edges(data = True, default = 0, keys=True):
            G.edges[nodo1, nodo2, keys]['P_R_Linea'] = 0
            G.edges[nodo1, nodo2, keys]['P_S_Linea'] = 0
            G.edges[nodo1, nodo2, keys]['P_T_Linea'] = 0
            G.edges[nodo1, nodo2, keys]['Q_R_Linea'] = 0
            G.edges[nodo1, nodo2, keys]['Q_S_Linea'] = 0
            G.edges[nodo1, nodo2, keys]['Q_T_Linea'] = 0
        
        #Reiniciar también a 0 las pérdidas de todos los enlaces.
        for nodo, data in G.nodes(data=True, default = 0):
            G.nodes[nodo]['P_R_0'] = 0
            G.nodes[nodo]['P_S_0'] = 0
            G.nodes[nodo]['P_T_0'] = 0
            G.nodes[nodo]['Q_R_0'] = 0
            G.nodes[nodo]['Q_S_0'] = 0
            G.nodes[nodo]['Q_T_0'] = 0
        
        #Se recorre el DF de AE para añadir la potencia de cada CUPS
        for index, row in df_AE_fecha.iterrows():
            if len(df_AE_fecha.loc[df_AE_fecha['CUPS'] == row.CUPS].reset_index(drop=True)) >= 1:
                if len(df_AE_fecha.loc[df_AE_fecha['CUPS'] == row.CUPS].reset_index(drop=True)) > 1:
                    logger.error('Encontrados ' + str(len(df_AE_fecha.loc[df_AE_fecha['CUPS'] == row.CUPS].reset_index(drop=True))) + ' filas con CCH_AE para el CUPS ' + str(row.CUPS) + ' y debería ser solo 1 fila. Se considera solo el primer valor para el análisis.')

                if df_AE_fecha.loc[df_AE_fecha['CUPS'] == row.CUPS].sort_values(colum_hora, ascending=False).reset_index(drop=True)[colum_hora][0] > 0:
                    # potencia_cup = float(df_AE_fecha.loc[df_AE_fecha['CUPS'] == row.CUPS][colum_hora].reset_index(drop=True)[0])
                    potencia_cup = float(df_AE_fecha.loc[df_AE_fecha['CUPS'] == row.CUPS].sort_values(colum_hora, ascending=False).reset_index(drop=True)[colum_hora][0])
                    #Cuidado con los posibles valores de potencia 'nan'.
                    if potencia_cup > 0:
                        aa = 'todo ok'
                        del aa
                    else:
                        potencia_cup = 0
                    Q_CUP = 0
                    cup_amm_fase = G.nodes[row.CUPS]['AMM_FASE']
                    if cup_amm_fase == 'R' or cup_amm_fase == 'S' or cup_amm_fase == 'T':
                        aa = 'todo ok'
                        del aa
                    else: 
                        logger.error('Error al identificar la fase ' + str(cup_amm_fase) + ' del CUPS ' + str(row.CUPS))
                        cup_amm_fase = 'R'
                        
                    cup_tipo_conexion = G.nodes[row.CUPS]['TIPO_CONEXION']
                    
                else:
                    potencia_cup = 0
                    Q_CUP = 0
                    cup_amm_fase = 'R'
                    cup_tipo_conexion = 'MONOFASICO'
                
                Nodo_grafo = str(list(G.edges(row.CUPS))[0][1])
                        
                #Se agrega la potencia en el nodo. Aquí se sumará lo de todos los CUPS conectados a ese nodo.
                #Se agrega siempre en P_FASE_0 y Q_FASE_0
                if cup_tipo_conexion == 'MONOFASICO':
                    #Se asigna el valor de potencia en el atributo del CUPS y en la fase correspondiente.
                    G.nodes[str(row.CUPS)]['P_' + cup_amm_fase + '_0'] = potencia_cup
                    G.nodes[str(row.CUPS)]['Q_' + cup_amm_fase + '_0'] = Q_CUP
                    #Idem. para la fase oportuna del nodo
                    G.nodes[Nodo_grafo]['P_' + cup_amm_fase + '_0'] = G.nodes[Nodo_grafo]['P_' + cup_amm_fase + '_0'] + potencia_cup
                    G.nodes[Nodo_grafo]['Q_' + cup_amm_fase + '_0'] = G.nodes[Nodo_grafo]['Q_' + cup_amm_fase + '_0'] + G.nodes[str(row.CUPS)]['Q_' + cup_amm_fase + '_0']
                    #Pérdidas de la línea que une la arqueta con el CUP. Puede ser trifásica o monofásica. NO tenemos el tipo de cable.
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0)]['P_' + cup_amm_fase + '_Linea'] = 0
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0)]['Q_' + cup_amm_fase + '_Linea'] = 0
                    
                elif cup_tipo_conexion == 'TRIFASICO':
                    #Se asigna el valor de potencia en el atributo del CUPS y en las 3 fases.
                    G.nodes[str(row.CUPS)]['P_R_0'] = potencia_cup/3
                    G.nodes[str(row.CUPS)]['Q_R_0'] = Q_CUP/3
                    G.nodes[str(row.CUPS)]['P_S_0'] = potencia_cup/3
                    G.nodes[str(row.CUPS)]['Q_S_0'] = Q_CUP/3
                    G.nodes[str(row.CUPS)]['P_T_0'] = potencia_cup/3
                    G.nodes[str(row.CUPS)]['Q_T_0'] = Q_CUP/3
                    
                    G.nodes[Nodo_grafo]['P_R_0'] = G.nodes[Nodo_grafo]['P_R_0'] + potencia_cup/3
                    G.nodes[Nodo_grafo]['Q_R_0'] = G.nodes[Nodo_grafo]['Q_R_0'] + Q_CUP/3
                    G.nodes[Nodo_grafo]['P_S_0'] = G.nodes[Nodo_grafo]['P_S_0'] + potencia_cup/3
                    G.nodes[Nodo_grafo]['Q_S_0'] = G.nodes[Nodo_grafo]['Q_S_0'] + Q_CUP/3
                    G.nodes[Nodo_grafo]['P_T_0'] = G.nodes[Nodo_grafo]['P_T_0'] + potencia_cup/3
                    G.nodes[Nodo_grafo]['Q_T_0'] = G.nodes[Nodo_grafo]['Q_T_0'] + Q_CUP/3
                    #Pérdidas de la línea que une la arqueta con el CUP. Puede ser trifásica o monofásica. NO tenemos el tipo de cable.
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0)]['P_R_Linea'] = 0
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0)]['Q_R_Linea'] = 0
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0)]['P_S_Linea'] = 0
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0]['Q_S_Linea'] = 0
                    # G.edges[(Nodo_grafo,  str(row.CUPS),0]['P_T_Linea'] = 0
                    # G.edges[(Nodo_grafo,  str(row.CUPS) ,0)]['Q_T_Linea'] = 0
                else:
                    logger.error('ERROR CUPS_AE. TIPO DE CONEXIÓN NO IDENTIFICADA (MONOFÁSICA/TRIFÁSICA): ' +  str(row.CUPS) + ': ' + str(cup_tipo_conexion))

        #Se repite el proceso para los CUPS con generación vertida a la red.
        #En este caso se define la potencia como negativa, de forma que se reste a la potencia inyectada por el trafo.
        for index, row in df_AS_fecha.iterrows():
            if len(df_AS_fecha.loc[df_AS_fecha['CUPS'] == row.CUPS].reset_index(drop=True)) >= 1:
                if len(df_AS_fecha.loc[df_AS_fecha['CUPS'] == row.CUPS].reset_index(drop=True)) > 1:
                    logger.error('Encontrados ' + str(len(df_AS_fecha.loc[df_AS_fecha['CUPS'] == row.CUPS].reset_index(drop=True))) + ' filas con CCH_AS para el CUPS ' + str(row.CUPS) + ' y debería ser solo 1 fila. Se considera solo el primer valor para el análisis.')
                       
                if df_AS_fecha.loc[df_AS_fecha['CUPS'] == row.CUPS].sort_values(colum_hora, ascending=False).reset_index(drop=True)[colum_hora][0] > 0:
                    potencia_cup = -1 * float(df_AS_fecha.loc[df_AS_fecha['CUPS'] == row.CUPS].sort_values(colum_hora, ascending=False).reset_index(drop=True)[colum_hora][0])
                    #Cuidado con los posibles valores de potencia 'nan'.
                    if potencia_cup < 0:
                        aa = 'todo ok'
                        del aa
                    else:
                        potencia_cup = 0
                    Q_CUP = 0
                    cup_amm_fase = G.nodes[row.CUPS]['AMM_FASE']
                    if cup_amm_fase == 'R' or cup_amm_fase == 'S' or cup_amm_fase == 'T':
                        aa = 'todo ok'
                        del aa
                    else: 
                        logger.error('Error al identificar la fase ' + str(cup_amm_fase) + ' del CUPS ' + str(row.CUPS))
                        cup_amm_fase = 'R'
                        
                    cup_tipo_conexion = G.nodes[row.CUPS]['TIPO_CONEXION']
                    
                else:
                    potencia_cup = 0
                    Q_CUP = 0
                    cup_amm_fase = 'R'
                    cup_tipo_conexion = 'MONOFASICO'
                
                Nodo_grafo = str(list(G.edges(row.CUPS))[0][1])
                        
                #Se agrega la potencia en el nodo. Aquí se sumará lo de todos los CUPS conectados a ese nodo. En este caso la generación se sumará con número negativo, por lo que se restará.
                #Se agrega siempre en P_FASE_0 y Q_FASE_0
                if cup_tipo_conexion == 'MONOFASICO':
                    #Se asigna el valor de potencia en el atributo del CUPS y en la fase correspondiente.
                    #Cuidado con los casos de autoconsumo, puede darse el caso de que, para una misma hora, un CUPS haya consumido e inyectado a la vez (EL PILAR 6720, 2020-10-03)
                    G.nodes[str(row.CUPS)]['P_' + cup_amm_fase + '_0'] += potencia_cup
                    G.nodes[str(row.CUPS)]['Q_' + cup_amm_fase + '_0'] += Q_CUP
                    #Idem. para la fase oportuna del nodo
                    G.nodes[Nodo_grafo]['P_' + cup_amm_fase + '_0'] = G.nodes[Nodo_grafo]['P_' + cup_amm_fase + '_0'] + potencia_cup
                    G.nodes[Nodo_grafo]['Q_' + cup_amm_fase + '_0'] = G.nodes[Nodo_grafo]['Q_' + cup_amm_fase + '_0'] + G.nodes[str(row.CUPS)]['Q_' + cup_amm_fase + '_0']
                    
                elif cup_tipo_conexion == 'TRIFASICO':
                    #Se asigna el valor de potencia en el atributo del CUPS y en las 3 fases.
                    G.nodes[str(row.CUPS)]['P_R_0'] += potencia_cup/3
                    G.nodes[str(row.CUPS)]['Q_R_0'] += Q_CUP/3
                    G.nodes[str(row.CUPS)]['P_S_0'] += potencia_cup/3
                    G.nodes[str(row.CUPS)]['Q_S_0'] += Q_CUP/3
                    G.nodes[str(row.CUPS)]['P_T_0'] += potencia_cup/3
                    G.nodes[str(row.CUPS)]['Q_T_0'] += Q_CUP/3
                    
                    G.nodes[Nodo_grafo]['P_R_0'] = G.nodes[Nodo_grafo]['P_R_0'] + potencia_cup/3
                    G.nodes[Nodo_grafo]['Q_R_0'] = G.nodes[Nodo_grafo]['Q_R_0'] + Q_CUP/3
                    G.nodes[Nodo_grafo]['P_S_0'] = G.nodes[Nodo_grafo]['P_S_0'] + potencia_cup/3
                    G.nodes[Nodo_grafo]['Q_S_0'] = G.nodes[Nodo_grafo]['Q_S_0'] + Q_CUP/3
                    G.nodes[Nodo_grafo]['P_T_0'] = G.nodes[Nodo_grafo]['P_T_0'] + potencia_cup/3
                    G.nodes[Nodo_grafo]['Q_T_0'] = G.nodes[Nodo_grafo]['Q_T_0'] + Q_CUP/3
                else:
                    logger.error('ERROR CUPS_AS. TIPO DE CONEXIÓN NO IDENTIFICADA (MONOFÁSICA/TRIFÁSICA): ' +  str(row.CUPS) + ': ' + str(cup_tipo_conexion))

        return G
    
    
    
    
    def resuelve_grafo(self, G, end_nodes_cups, id_ct, splitting_nodes_sin_cups, nodos_cups_descendientes, temp_cables, CCH_Data_Error):
        """
        
        Función para resolver el grafo y calcular pérdidas a partir de los datos introducidos. Emplea la librería cables.py para obtener la resistencia para el tipo de cable de cada traza.
        
        Parámetros
        ----------
        G : Grafo definido.
        end_nodes_cups : Listado con todos los nodos que pueden ser terminación de línea y/o tener CUPs conectados.
        id_ct : Identificador numérico del CT. Se obtiene del archivo de Trazas.
        splitting_nodes_sin_cups : Listado con todos los nodos que tienen más de un predecesor-sucesor.
        nodos_cups_descendientes : Listado de nodos con CUPS conectados y que a su vez no son terminaciones de línea.
        temp_cables : Temperatura a considerar para los cables. Por defecto 20ºC.
        CCH_Data_Error : Parámetro para controlar la veracidad de los resultados obtenidos.
        
        
        Retorno
        -------
        G : Grafo actualizado con los datos de potencias agregadas en cada nodo y de pérdidas en cada traza.
        CCH_Data_Error : Valor del parámetro de comprobación de resultados.
        """
        logger = logging.getLogger('resuelve_grafo')
        #Hay que asegurarse de que todos los nodos tienen al atributo 'Enlaces_iter' igual que 'Enlaces_orig'
        for row in G.nodes:
            if G.nodes[row]['Tipo_Nodo'] != 'CUPS' and G.nodes[row]['Tipo_Nodo'] != 'CUPS_TR':
                G.nodes[row]['Enlaces_iter'] = G.nodes[row]['Enlaces_orig']
        
        #Se recorren todos los nodos finales (los que no tienen conectado nada y los que tienen conectados los CUPS pero NO tienen otros descendientes)
        for row in end_nodes_cups:
            ruta=list(nx.shortest_path(G,str(self.id_ct),row))
            ruta.remove(row)
            
            #Se recorre la ruta en sentido inverso, añadiendo la potencia y calculando las pérdidas de los vanos.
            #Se para cuando se encuentra un nodo con más ramificaciones.
            nodo_old = row
            for row2 in reversed(ruta):
                N_anteriores = 0
                for i in range(0,1000000):
                    try:
                        G.edges[(row2, nodo_old, i)]
                        N_anteriores += 1
                    except:
                        break

                for i in range(0, N_anteriores):
                    tipo_cable = str(G.edges[(row2, nodo_old,i)]['CABLE'])
                    QBT_TENSION = str(G.edges[(row2, nodo_old,i)]['QBT_TENSION'])
                    if QBT_TENSION == '400':
                        V_Linea = self.V_Linea_400
                    elif QBT_TENSION == '230':
                        V_Linea = self.V_Linea_230
                    I_R_row = (math.sqrt(float(G.nodes[nodo_old]['P_R_0']*1000)**2 + float(G.nodes[nodo_old]['Q_R_0']*1000)**2)/(V_Linea/math.sqrt(3)))/N_anteriores
                    I_S_row = (math.sqrt(float(G.nodes[nodo_old]['P_S_0']*1000)**2 + float(G.nodes[nodo_old]['Q_S_0']*1000)**2)/(V_Linea/math.sqrt(3)))/N_anteriores
                    I_T_row = (math.sqrt(float(G.nodes[nodo_old]['P_T_0']*1000)**2 + float(G.nodes[nodo_old]['Q_T_0']*1000)**2)/(V_Linea/math.sqrt(3)))/N_anteriores
                
                    #Se llama a la librería de cálculo de resistencia por km
                    Cable_R = cable.Conductor()
        #                Cable_R.fload_library( "RZ_0.6/1.0_kV_4x10_CU")
                    Found_cable = Cable_R.fload_library( tipo_cable)
                    # if Found_cable == 0:
                    #     logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '-' + str(i) + '. Se consideran valores definidos por defecto.')
                    Cable_R.fset_t1(temp_cables)
                    Cable_R.fset_i(I_R_row)
                    R_cable_ohm_km_R = Cable_R.fcompute_r()
                    
                    # longitud = float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))
                    # if longitud > 100:
                    #     longitud = 100
                    #     logger.warning('La traza ' + str(row.NODO_ORIGEN) + ' - ' + str(row.NODO_DESTINO) + ' tiene más de 100m de longitud. Posible error, se asigna longitud=100m.')
                
                    R_cable_ohm_R = R_cable_ohm_km_R * float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))/1000
                    
                    Cable_S = cable.Conductor()
                    Found_cable = Cable_S.fload_library( tipo_cable)
                    # if Found_cable == 0:
                    #     logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '-' + str(i) + '. Se consideran valores definidos por defecto.')
                    Cable_S.fset_t1(temp_cables)
                    Cable_S.fset_i(I_S_row)
                    R_cable_ohm_km_S = Cable_S.fcompute_r()
                    R_cable_ohm_S = R_cable_ohm_km_S * float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))/1000
                    
                    Cable_T = cable.Conductor()
                    Found_cable = Cable_T.fload_library( tipo_cable)
                    # if Found_cable == 0:
                    #     logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '-' + str(i) + '. Se consideran valores definidos por defecto.')
                    Cable_T.fset_t1(temp_cables)
                    Cable_T.fset_i(I_T_row)
                    R_cable_ohm_km_T = Cable_T.fcompute_r()
                    R_cable_ohm_T = R_cable_ohm_km_T * float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))/1000
                        
                        
                    P_R_row = R_cable_ohm_R*I_R_row**2/1000 #Se pasa a kW
                    Q_R_row = self.X_cable*I_R_row**2/1000
                    P_S_row = R_cable_ohm_S*I_S_row**2/1000 #Se pasa a kW
                    Q_S_row = self.X_cable*I_S_row**2/1000
                    P_T_row = R_cable_ohm_T*I_T_row**2/1000 #Se pasa a kW                    
                    Q_T_row = self.X_cable*I_T_row**2/1000
                
                        
                    #Si el nodo destino tiene bifurcación hay que tener cuidado de no agregar potencia en el ID que no corresponde. En el caso de los enlaces es igual en todos los casos.
                    #Las pérdidas dan un valor positivo aunque la potencia en nodo_old sea negativa (autoconsumo).
                    #Hay que mantenerlas positivas para que al sumar P_Nodo_old + Pérdidas sea un número negativo más grande (P_row2 será mayor que P_nodo_old)
                    #Pero al definir las pérdidas en el grafo se hace en negativo, para indicar que son debidas a autoconsumo y que no se asocian al grafo.
                    if G.nodes[nodo_old]['P_R_0'] < 0:
                        G.edges[(row2, nodo_old,i)]['P_R_Linea'] = G.edges[(row2, nodo_old,i)]['P_R_Linea'] - P_R_row
                        G.edges[(row2, nodo_old,i)]['Q_R_Linea'] = G.edges[(row2, nodo_old,i)]['Q_R_Linea'] - Q_R_row
                    else:
                        G.edges[(row2, nodo_old,i)]['P_R_Linea'] = G.edges[(row2, nodo_old,i)]['P_R_Linea'] + P_R_row
                        G.edges[(row2, nodo_old,i)]['Q_R_Linea'] = G.edges[(row2, nodo_old,i)]['Q_R_Linea'] + Q_R_row
                    if G.nodes[nodo_old]['P_S_0'] < 0:
                        G.edges[(row2, nodo_old,i)]['P_S_Linea'] = G.edges[(row2, nodo_old,i)]['P_S_Linea'] - P_S_row
                        G.edges[(row2, nodo_old,i)]['Q_S_Linea'] = G.edges[(row2, nodo_old,i)]['Q_S_Linea'] - Q_S_row
                    else:
                        G.edges[(row2, nodo_old,i)]['P_S_Linea'] = G.edges[(row2, nodo_old,i)]['P_S_Linea'] + P_S_row
                        G.edges[(row2, nodo_old,i)]['Q_S_Linea'] = G.edges[(row2, nodo_old,i)]['Q_S_Linea'] + Q_S_row
                    if G.nodes[nodo_old]['P_T_0'] < 0:
                        G.edges[(row2, nodo_old,i)]['P_T_Linea'] = G.edges[(row2, nodo_old,i)]['P_T_Linea'] - P_T_row
                        G.edges[(row2, nodo_old,i)]['Q_T_Linea'] = G.edges[(row2, nodo_old,i)]['Q_T_Linea'] - Q_T_row
                    else:
                        G.edges[(row2, nodo_old,i)]['P_T_Linea'] = G.edges[(row2, nodo_old,i)]['P_T_Linea'] + P_T_row
                        G.edges[(row2, nodo_old,i)]['Q_T_Linea'] = G.edges[(row2, nodo_old,i)]['Q_T_Linea'] + Q_T_row
                    
                    #En el caso de la potencia en los nodos:
                    #Cuidado con sumar varias veces cuando i > 0
                    if i == 0:
                        G.nodes[row2]['P_R_0'] = G.nodes[row2]['P_R_0'] + G.nodes[nodo_old]['P_R_0'] + P_R_row
                        G.nodes[row2]['Q_R_0'] = G.nodes[row2]['Q_R_0'] + G.nodes[nodo_old]['Q_R_0'] + Q_R_row
                        G.nodes[row2]['P_S_0'] = G.nodes[row2]['P_S_0'] + G.nodes[nodo_old]['P_S_0'] + P_S_row
                        G.nodes[row2]['Q_S_0'] = G.nodes[row2]['Q_S_0'] + G.nodes[nodo_old]['Q_S_0'] + Q_S_row
                        G.nodes[row2]['P_T_0'] = G.nodes[row2]['P_T_0'] + G.nodes[nodo_old]['P_T_0'] + P_T_row
                        G.nodes[row2]['Q_T_0'] = G.nodes[row2]['Q_T_0'] + G.nodes[nodo_old]['Q_T_0'] + Q_T_row
                    else:
                        G.nodes[row2]['P_R_0'] = G.nodes[row2]['P_R_0'] + P_R_row
                        G.nodes[row2]['Q_R_0'] = G.nodes[row2]['Q_R_0'] + Q_R_row
                        G.nodes[row2]['P_S_0'] = G.nodes[row2]['P_S_0'] + P_S_row
                        G.nodes[row2]['Q_S_0'] = G.nodes[row2]['Q_S_0'] + Q_S_row
                        G.nodes[row2]['P_T_0'] = G.nodes[row2]['P_T_0'] + P_T_row
                        G.nodes[row2]['Q_T_0'] = G.nodes[row2]['Q_T_0'] + Q_T_row
                
                G.nodes[nodo_old]['Enlaces_iter'] -=1
                
                if row2 in splitting_nodes_sin_cups:
                    G.nodes[row2]['Enlaces_iter'] -=1
                    break 
                
                nodo_old = row2
                
        
        #Ahora se recorre de forma iterativa los splitting nodes que ya tienen 'Enlaces_iter' == 1 hasta que se llega a otro splitting node con más ramas.
        #Así hasta acabar con todo el grafo.            
        nodos_utilizados = []
        a=0
        salir_bucle = 0
        while len(nodos_utilizados) < len(splitting_nodes_sin_cups) and salir_bucle == 0 :#and a == 0:     
            a = 1
            for row in splitting_nodes_sin_cups:
                if G.nodes[row]['Enlaces_iter'] == 1 and (row not in (nodos_utilizados)):
                    a = 0
                    ruta=list(nx.shortest_path(G,str(self.id_ct),row))
                    ruta.remove(row)
                    #Se recorre la ruta en sentido inverso, añadiendo la potencia y calculando las pérdidas de los vanos.
                    #Hasta que se encuentra un nodo con más ramificaciones
                    nodo_old = row
                    for row2 in reversed(ruta):
                        #Es necesario en todos los casos identificar los diferentes ID de potencia que existen en el nodo y continuar aguas arriba por el enlace adecuado.
                        N_anteriores = 0
                        for i in range(0,1000000):
                            try:
                                G.edges[(row2, nodo_old, i)]
                                N_anteriores += 1
                            except:
                                break

                        for i in range(0,N_anteriores):
                            #Si la longitud de la traza es distinta de 0 se calculan las pérdidas. si es 0 puede ser un enlace del CT virtual que no tiene asignado tipo de cable
                            tipo_cable = str(G.edges[(row2, nodo_old,i)]['CABLE'])
                            QBT_TENSION = str(G.edges[(row2, nodo_old,i)]['QBT_TENSION'])
                            if QBT_TENSION == '400':
                                V_Linea = self.V_Linea_400
                            elif QBT_TENSION == '230':
                                V_Linea = self.V_Linea_230
                            I_R_row = (math.sqrt(float(G.nodes[nodo_old]['P_R_0']*1000)**2 + float(G.nodes[nodo_old]['Q_R_0']*1000)**2)/(V_Linea/math.sqrt(3)))/N_anteriores
                            I_S_row = (math.sqrt(float(G.nodes[nodo_old]['P_S_0']*1000)**2 + float(G.nodes[nodo_old]['Q_S_0']*1000)**2)/(V_Linea/math.sqrt(3)))/N_anteriores
                            I_T_row = (math.sqrt(float(G.nodes[nodo_old]['P_T_0']*1000)**2 + float(G.nodes[nodo_old]['Q_T_0']*1000)**2)/(V_Linea/math.sqrt(3)))/N_anteriores   
                            
                            # I_R_row = (math.sqrt(3)*math.sqrt(float(G.nodes[nodo_old]['P_R_0']*1000)**2 + float(G.nodes[nodo_old]['Q_R_0']*1000)**2)/(math.sqrt(3)*self.V_Linea))/N_anteriores
                            # I_S_row = (math.sqrt(3)*math.sqrt(float(G.nodes[nodo_old]['P_S_0']*1000)**2 + float(G.nodes[nodo_old]['Q_S_0']*1000)**2)/(math.sqrt(3)*self.V_Linea))/N_anteriores
                            # I_T_row = (math.sqrt(3)*math.sqrt(float(G.nodes[nodo_old]['P_T_0']*1000)**2 + float(G.nodes[nodo_old]['Q_T_0']*1000)**2)/(math.sqrt(3)*self.V_Linea))/N_anteriores   
                            
                            #Se llama a la librería de cálculo de resistencia por km
                            Cable_R = cable.Conductor()
                            Found_cable = Cable_R.fload_library( tipo_cable)
                            # if Found_cable == 0:
                            #     logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '-' + str(i) + '. Se consideran valores definidos por defecto.')
                            Cable_R.fset_t1(temp_cables)
                            Cable_R.fset_i(I_R_row)
                            R_cable_ohm_km_R = Cable_R.fcompute_r()
                            R_cable_ohm_R = R_cable_ohm_km_R * float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))/1000
                            
                            Cable_S = cable.Conductor()
                            Found_cable = Cable_S.fload_library( tipo_cable)
                            # if Found_cable == 0:
                            #     logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '-' + str(i) + '. Se consideran valores definidos por defecto.')
                            Cable_S.fset_t1(temp_cables)
                            Cable_S.fset_i(I_S_row)
                            R_cable_ohm_km_S = Cable_S.fcompute_r()
                            R_cable_ohm_S = R_cable_ohm_km_S * float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))/1000
                            
                            Cable_T = cable.Conductor()
                            Found_cable = Cable_T.fload_library( tipo_cable)
                            # if Found_cable == 0:
                            #     logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '-' + str(i) + '. Se consideran valores definidos por defecto.')
                            Cable_T.fset_t1(temp_cables)
                            Cable_T.fset_i(I_T_row)
                            R_cable_ohm_km_T = Cable_T.fcompute_r()
                            R_cable_ohm_T = R_cable_ohm_km_T * float(str(G.edges[(row2, nodo_old,i)]['Long']).replace(',','.'))/1000
                            
                        
                            P_R_row = R_cable_ohm_R*I_R_row**2/1000 #Se pasa a kW
                            Q_R_row = self.X_cable*I_R_row**2/1000
                            P_S_row = R_cable_ohm_S*I_S_row**2/1000 #Se pasa a kW
                            Q_S_row = self.X_cable*I_S_row**2/1000
                            P_T_row = R_cable_ohm_T*I_T_row**2/1000 #Se pasa a kW
                            Q_T_row = self.X_cable*I_T_row**2/1000  
                            
                            #Las pérdidas dan un valor positivo aunque la potencia en nodo_old sea negativa (autoconsumo).
                            #Hay que mantenerlas positivas para que al sumar P_Nodo_old + Pérdidas sea un número negativo más grande (P_row2 será mayor que P_nodo_old)
                            #Pero al definir las pérdidas en el grafo se hace en negativo, para indicar que son debidas a autoconsumo y que no se asocian al grafo.
                            if G.nodes[nodo_old]['P_R_0'] < 0:
                                G.edges[(row2, nodo_old,i)]['P_R_Linea'] = G.edges[(row2, nodo_old,i)]['P_R_Linea'] - P_R_row
                                G.edges[(row2, nodo_old,i)]['Q_R_Linea'] = G.edges[(row2, nodo_old,i)]['Q_R_Linea'] - Q_R_row
                            else:
                                G.edges[(row2, nodo_old,i)]['P_R_Linea'] = G.edges[(row2, nodo_old,i)]['P_R_Linea'] + P_R_row
                                G.edges[(row2, nodo_old,i)]['Q_R_Linea'] = G.edges[(row2, nodo_old,i)]['Q_R_Linea'] + Q_R_row
                            if G.nodes[nodo_old]['P_S_0'] < 0:
                                G.edges[(row2, nodo_old,i)]['P_S_Linea'] = G.edges[(row2, nodo_old,i)]['P_S_Linea'] - P_S_row
                                G.edges[(row2, nodo_old,i)]['Q_S_Linea'] = G.edges[(row2, nodo_old,i)]['Q_S_Linea'] - Q_S_row
                            else:
                                G.edges[(row2, nodo_old,i)]['P_S_Linea'] = G.edges[(row2, nodo_old,i)]['P_S_Linea'] + P_S_row
                                G.edges[(row2, nodo_old,i)]['Q_S_Linea'] = G.edges[(row2, nodo_old,i)]['Q_S_Linea'] + Q_S_row
                            if G.nodes[nodo_old]['P_T_0'] < 0:
                                G.edges[(row2, nodo_old,i)]['P_T_Linea'] = G.edges[(row2, nodo_old,i)]['P_T_Linea'] - P_T_row
                                G.edges[(row2, nodo_old,i)]['Q_T_Linea'] = G.edges[(row2, nodo_old,i)]['Q_T_Linea'] - Q_T_row
                            else:
                                G.edges[(row2, nodo_old,i)]['P_T_Linea'] = G.edges[(row2, nodo_old,i)]['P_T_Linea'] + P_T_row
                                G.edges[(row2, nodo_old,i)]['Q_T_Linea'] = G.edges[(row2, nodo_old,i)]['Q_T_Linea'] + Q_T_row
                            
                            
                            #En el caso de la potencia en los nodos:
                            #Cuidado con sumar varias veces cuando i > 0
                            if i == 0:
                                G.nodes[row2]['P_R_0'] = G.nodes[row2]['P_R_0'] + G.nodes[nodo_old]['P_R_0'] + P_R_row
                                G.nodes[row2]['Q_R_0'] = G.nodes[row2]['Q_R_0'] + G.nodes[nodo_old]['Q_R_0'] + Q_R_row
                                G.nodes[row2]['P_S_0'] = G.nodes[row2]['P_S_0'] + G.nodes[nodo_old]['P_S_0'] + P_S_row
                                G.nodes[row2]['Q_S_0'] = G.nodes[row2]['Q_S_0'] + G.nodes[nodo_old]['Q_S_0'] + Q_S_row
                                G.nodes[row2]['P_T_0'] = G.nodes[row2]['P_T_0'] + G.nodes[nodo_old]['P_T_0'] + P_T_row
                                G.nodes[row2]['Q_T_0'] = G.nodes[row2]['Q_T_0'] + G.nodes[nodo_old]['Q_T_0'] + Q_T_row
                            else:
                                G.nodes[row2]['P_R_0'] = G.nodes[row2]['P_R_0'] + P_R_row
                                G.nodes[row2]['Q_R_0'] = G.nodes[row2]['Q_R_0'] + Q_R_row
                                G.nodes[row2]['P_S_0'] = G.nodes[row2]['P_S_0'] + P_S_row
                                G.nodes[row2]['Q_S_0'] = G.nodes[row2]['Q_S_0'] + Q_S_row
                                G.nodes[row2]['P_T_0'] = G.nodes[row2]['P_T_0'] + P_T_row
                                G.nodes[row2]['Q_T_0'] = G.nodes[row2]['Q_T_0'] + Q_T_row
                            
                        G.nodes[nodo_old]['Enlaces_iter'] -=1
                        
                        
                        if row2 in splitting_nodes_sin_cups:
                            if G.nodes[row2]['Enlaces_iter'] > 2:
                                G.nodes[row2]['Enlaces_iter'] -=1
                                salir_bucle = 1
                                break 
                            nodos_utilizados.append(row2)                        
                        
                        nodo_old = row2
                        
                            
                    nodos_utilizados.append(row)
                    nodos_utilizados = list(dict.fromkeys(nodos_utilizados))
            if a == 1:
                print('Entramos en un bucle...')
                logger.critical('Error encontrado. Entrada en bucle no resuelto. se aborta el cálculo de pérdidas.')
                CCH_Data_Error = 4
                break
                
        return G, CCH_Data_Error
    
   
    
    



##############################################################################
## Código main
##############################################################################
def main(self):
    """
        
    Función principal.
    
    Parámetros
    ----------
    Los globales de la librería.
        
    
    Retorno
    -------
    Posible almacenamiento de resultados en la BBDD SQL si se define el parámetro de entrada oportuno (save_ddbb).
    graph_data_error : Valor actualizado.
    df_nodos_ct : DataFrame con los nodos del CT a analizar con los posibles errores corregidos.
    df_traza_ct : DataFrame con las trazas del CT a analizar con los posibles errores corregidos.
    df_ct_cups_ct : DataFrame con la relación de CUPS del CT a analizar con los posibles errores corregidos.
    df_matr_dist : DataFrame con los CUPS del CT asociados al nodo más cercano que coincide con el trafo al que está conectado.
    LBT_ID_list : DataFrame con el listado de LBTs que componen el grafo.
    cups_agregado_CT : DataFrame con los CUPS de la cabecera del CT.
    """
    #Diccionario de horas con los nombres de todas las columnas de las curgas de carga
    diccionario_horas = {'VALOR_H01': '01', 'VALOR_H02': '02', 'VALOR_H03': '03', 'VALOR_H04': '04', 'VALOR_H05': '05', 'VALOR_H06': '06', 'VALOR_H07': '07', 'VALOR_H08': '08', 'VALOR_H09': '09', 'VALOR_H10': '10', 'VALOR_H11': '11', 'VALOR_H12': '12', 'VALOR_H13': '13', 'VALOR_H14': '14', 'VALOR_H15': '15', 'VALOR_H16': '16', 'VALOR_H17': '17', 'VALOR_H18': '18', 'VALOR_H19': '19', 'VALOR_H20': '20', 'VALOR_H21': '21', 'VALOR_H22': '22', 'VALOR_H23': '23', 'VALOR_H24': '00'}#, 'VALOR_H25': '25'}
    #Diccionario de colores para asignar a los nodos
    dicc_colors = {'CT/CT_LBT': 'red', 'ARQUETA': 'blue', 'CGP': 'black', 'DERIVACION': 'yellow', 'APOYO': 'orange', 'CAMBIO_CABLE': 'brown', 'AGARRE': 'lime', 'PUNTO': 'cyan', 'GENERADOR': 'lightseagreen', 'ACOPLE': 'purple', 'NODO_DESCONOCIDO': 'gray', 'SIN_NODO': 'white', 'CUPS': 'green', 'CUPS_TR': 'salmon'}
    
    plt_graph_file = self.ruta_raiz + 'images_files/Graph_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '_LBT.jpg'
    plt_graph_file_v2 = self.ruta_raiz + 'images_files/Graph_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '_geograph.jpg'
    
    # Se sobreescriben los handlers anteriores de este CT
    if logging.getLogger('').hasHandlers():
        logging.getLogger('').handlers.clear()
    
    logging.basicConfig(filename=self.ruta_log, level=eval(self.log_mode), filemode='w')
    logger = logging.getLogger(__name__)
    logger.setLevel(eval(self.log_mode))
    
    logger.info('###################################################################')
    logger.info('Ejecutado el ' + str(time.strftime("%d/%m/%y")) + ' a las ' + str(time.strftime("%H:%M:%S")))
    logger.info('CT seleccionado: ' + str(self.Nombre_CT))
    logger.info('V_Linea_400=' + str(self.V_Linea_400) + ', V_Linea_230=' + str(self.V_Linea_230) + ', X_cable=' + str(self.X_cable) + ', temp_cables=' + str(self.temp_cables))
    logger.info('###################################################################')
    logger.info('Archivo topología: ' + str(self.archivo_topologia) + '. Archivo trazas: ' + str(self.archivo_traza) + '. Archivo CUPS: ' + str(self.archivo_ct_cups) + '. Ruta curvas de carga: ' + str(self.ruta_cch) + '. Archivo config. SQL: ' + str(self.archivo_config) + '. Guardado de las imagenes del grafo: ' + str(self.save_plt_graph) + '. Imagen 1: ' + str(plt_graph_file) + ', imagen 2: ' + str(plt_graph_file_v2) + '. Guardado de resultados en SQL: ' + str(self.save_ddbb) + '. Logging mode: ' + str(self.log_mode))
    

    ##############################################################################
    ## Lectura de datos para la conexión con la DB SQL.
    ##############################################################################
    try:
        f = open (self.archivo_config,'r')
        ip_server = f.readline().splitlines()[0]
        db_server = f.readline().splitlines()[0]
        usr_server = f.readline().splitlines()[0]
        pwd_server = f.readline().splitlines()[0]
        f.close()
    except:
        logger.critical('Error al leer los datos de conexión con la DDBB.')
        
        
    ##############################################################################
    ## Uso de un archivo .gml o .gml.gz con un grafo ya definido que contenga una descripción previa de la red.
    ## Agiliza mucho el análisis.
    ##############################################################################
    gml_ok = 0 #Controla si se ha leído correctamente el .gml o hay que intentar generar el grafo desde el principio.
    graph_data_error = 0 #Control de la calidad de generación del grafo. Si se pone a 1 es que hay errores simples, 2 errores importantes pero corregibles, 3 imposible formar un grafo válido para analizar las pérdidas y se aborta el código.
    if self.use_gml_file == 0:
        try:
            #G = nx.read_gml(self.ruta_raiz + 'gml_files/' + 'Graph_def_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '.gml')
            G = nx.read_gml(self.ruta_raiz + 'gml_files/' + 'Graph_def_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '.gml.gz')
            graph_data_error = G.nodes[str(self.id_ct)]['Graph_ok']
            if graph_data_error == 3:
                logger.error('.gml leído pero con errores críticos. Se intentará volver a generar el grafo.')
            else:
                logger.debug('.gml leído correctamente.')
            
        except:
            gml_ok = 1
            logger.error('Error al intentar cargar el archivo .gml.gz desde ' + self.ruta_raiz + 'gml_files/' + 'Graph_def_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '.gml.gz. Revise el archivo, directorio y nombre. Se intentará crear el grafo y generar un .gml.gz nuevo.')
    
        
    
    ##############################################################################
    ## Creación del grafo desde el principio.
    ## En caso de leer el grafo de un .gml o gml.gz se omite este paso, salvo que haya habido un error al leerlo, que se intenta generar desde el principio.
    ##############################################################################
    if self.use_gml_file == 1 or gml_ok == 1: #or graph_data_error == 3:
        ##############################################################################
        ## Creación de DataFrames con las tablas para definir el grafo.
        ## En caso de leer el grafo de un .gml o gml.gz se omite este paso.
        ##############################################################################
        graph_data_error, df_nodos_ct, df_traza_ct, df_ct_cups_ct, df_matr_dist, LBT_ID_list, cups_agregado_CT = self.create_graph_dataframes(graph_data_error, self.archivo_topologia, self.archivo_traza, self.archivo_ct_cups)
        
        #Si ha habido un error que indique graph_data_error = 3 al llamar a la función anterior, habrá devuelto este valor y todo lo demás a 0. Se comprueba que varios de los parámetros son 0 y se sale de la librería.
        if graph_data_error == 3 and len(df_traza_ct) == 0 and len(df_matr_dist) == 0 and len(df_ct_cups_ct) == 0:
            logger.critical('Error crítico en los datos del .csv de la red. Se aborta el código.')
            return
        
        ##############################################################################
        ## Generación de un multigrafo con los DF obtenidos.
        ## Se empiezan añadiendo nodos y trazas y después los CUPS.
        ##############################################################################
        # Se genera el grafo y se llama a la función que añade nodos y enlaces.
        #G = nx.DiGraph() #Grafo dirigido
        # G = nx.Graph() #Grafo NO dirigido
        G = nx.MultiGraph() #Multigrafo no dirigido. Permite añadir múltiples enlaces entre los mismos nodos, con diferentes valores en los atributos.
        
        #Se genera el grafo con los DF creados
        G, graph_data_error = self.genera_grafo(G, df_nodos_ct, df_traza_ct, LBT_ID_list, cups_agregado_CT, dicc_colors, graph_data_error)
    
        ##############################################################################
        ## Lectura de los archivos de CUPS y asociación de CUPS con el nodo correpsondiente del grafo.
        ##############################################################################
        if len(df_ct_cups_ct) > 0:
            G = self.add_cups_grafo(G, df_ct_cups_ct, df_matr_dist, self.id_ct, LBT_ID_list, dicc_colors)
        else:
            if graph_data_error <= 3: #Es necesario comprobar que no tiene un valor mayor (más defectos en el grafo), para no sustituirlo por un valor menor.
                graph_data_error = 3
                logger.critical('Error crítico al no encontrar CUPS en la red.')
        
        
        #Se añade en el nodo CT un atributo que indica la calidad de los nodos del grafo:
        G.nodes[str(self.id_ct)]['Graph_ok'] = graph_data_error
                    
        
        ##############################################################################
        ## Guardado de los archivos .csv modificados con la información de la red.
        ##############################################################################
        if self.save_csv_mod == 0:
            try:
                df_matr_dist.to_csv(self.ruta_raiz + 'csv_files/' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '_Matr_Dist.csv', index = False, encoding='Latin9', sep=';', decimal=',')
                df_nodos_ct.to_csv(self.ruta_raiz + 'csv_files/' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '_Nodos_mod.csv', index = False, encoding='Latin9', sep=';', decimal=',')
                df_traza_ct.to_csv(self.ruta_raiz + 'csv_files/' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '_Traza_mod.csv', index = False, encoding='Latin9', sep=';', decimal=',')
                df_ct_cups_ct.to_csv(self.ruta_raiz + 'csv_files/' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '_CT_CUPS_mod.csv', index = False, encoding='Latin9', sep=';', decimal=',')
                logger.debug('Guardados correctamente los cuatro archivos .csv de descripción del grafo en la carpeta ' + self.ruta_raiz + 'csv_files/')
            except:
                logger.error('Error al guardar los cuatro archivos .csv de descripción del grafo en la carpeta ' + self.ruta_raiz + 'csv_files/')
                
    
    
    
        ##############################################################################
        ## Generación de un archivo .gml con la descripción del grafo.
        ##############################################################################
        # https://networkx.org/documentation/stable/reference/readwrite/index.html
        # https://networkx.org/documentation/stable/reference/readwrite/generated/networkx.readwrite.gml.read_gml.html
        #Archivo sin comprimir. Ej: El_Infierno = 60 kB
        # nx.write_gml(G, self.ruta_raiz + 'gml_files/' + 'Graph_def_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '.gml')
        # nx.write_yaml(G, self.ruta_raiz + 'Graph_def_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '.yaml')
        #Archivo comprimido. Ej. El_Infierno = 4,3 kB
        #Se guarda si no ha habido un error crítico de descripción que no permita tener un 
        # if graph_data_error != 3:
        try:
            nx.write_gml(G, self.ruta_raiz + 'gml_files/' + 'Graph_def_' + self.Nombre_CT.replace(' ', '_') + '_' + str(self.id_ct) + '.gml.gz')
            logger.debug('Guardado correctamente el archivo .gml.gz con la descripción del grafo en la carpeta ' + self.ruta_raiz + 'gml_files/')
        except:
            logger.error('Error al guardar el archivo .gml.gz con la descripción del grafo en la carpeta ' + self.ruta_raiz + 'gml_files/')
                

    
    ##############################################################################
    ## Representación gráfica de la red
    ##############################################################################
    if self.save_plt_graph == 0:
        try:
            plt.close()
            plt.subplot(111)
            posicion = nx.get_node_attributes(G,'pos')
            color_map = []
            #for node in G:
            for nodo, data in G.nodes(data=True, default = 0):
                color_map.append(data['color_nodo'])
            # color1 = nx.get_node_attributes(G,'color_nodo')
            # node_color=color1.values()
            #nx.draw(G, with_labels=True)
            #nx.draw(G, cmap = plt.get_cmap('jet'), with_labels=True, pos=nx.spring_layout(G))
            #nx.draw_networkx_edges(G, pos=nx.spring_layout(G))
            #nx.draw_networkx_nodes(G, pos=nx.spring_layout(G))    
            nx.draw_kamada_kawai(G, node_size=7, node_color=color_map)
    #        nx.draw(G, pos=pos)
        #    plt.show(block=False)                
            plt.savefig(plt_graph_file, format="JPG", dpi=800, bbox_inches='tight')
            plt.close()
            logger.debug('Guardada la representación eléctrica de la red en el archivo ' + str(plt_graph_file))
            
            plt.subplot(111)
            nx.draw(G, posicion, node_size=7, node_color=color_map)
            #nx.draw(G, node_size=7)
            plt.savefig(plt_graph_file_v2, format="JPG", dpi=800, bbox_inches='tight')
            plt.close()
            logger.debug('Guardada la representación geográfica de la red en el archivo ' + str(plt_graph_file_v2))
        #    plt.draw()
        
            #Se pinta la leyenda de colores en otra figura:
            # plt.subplot(111)
            # for clave in dicc_colors:
            #     plt.plot(0, 0, 'o', markersize=7, color=dicc_colors[clave], label=clave)
            # legend=plt.legend(title='Tipo de nodo', title_fontsize=20, loc=10, fontsize =20, markerscale=1, framealpha=1, borderpad=1, shadow=None)
            # ##plt.savefig(plt_graph_file_v2+'w.jpg', format="JPG", dpi=300, bbox_inches='tight')
            # fig  = legend.figure
            # fig.canvas.draw()
            # bbox  = legend.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
            # fig.savefig(plt_graph_file_v2+'_legend.jpg', dpi="figure", bbox_inches=bbox)
            # plt.close()
            
            # plt.subplot(111)
            # colores_red = [y['color_nodo'] for x,y in G.nodes(data=True)]
            # colores_red = list(dict.fromkeys(colores_red))
            # for color in colores_red:
            #     nodos_color = [x for x,y in G.nodes(data=True) if y['color_nodo']=='red']
            #     nx.draw(G, nodelist=nodos_color, node_size=7, node_color=color_map, label=color)
            # plt.legend()
            # plt.savefig(plt_graph_file_v2+'w.jpg', format="JPG", dpi=300, bbox_inches='tight')
            # plt.close()
        except:
            logger.error('Error al generar las imágenes .jpg con la descripción del grafo en ' + plt_graph_file + ' y ' + plt_graph_file_v2)
    
    print('Graph_data_error = ' + str(graph_data_error))
    self.update_graph_data_error(graph_data_error)
    
    if graph_data_error == 3:
        logger.critical('CREACIÓN DEL GRAFO ABORTADA. ERRORES INCOMPATIBLES CON UNA CORRECTA DEFINICIÓN.')
        print('CREACIÓN DEL GRAFO ABORTADA. ERRORES INCOMPATIBLES CON UNA CORRECTA DEFINICIÓN. Graph_data_error = ' + str(graph_data_error))
        self.update_graph_data_error(graph_data_error)
        return
        # return graph_data_error

    
    ##############################################################################
    ## Creación de listas de nodos necesarias para resolver el grafo.
    ##############################################################################
    ##Se crea una lista con los nodos que tienen CUPS conectados y nada más aguas abajo y otra lista con nodos que tienen cups pero tienen más nodos descendientes.
    nodos_cups_conectados = [] #Se guardan todos los nodos que tienen al menos un CUP conectado y NO tienen otra rama con descendientes
    nodos_cups_descendientes = []#Se guardan los nodos que tienen CUPs conectados y SI tienen ramas con descendientes.
    #Identificar todos los nodos que tienen más de un predecesor-sucesor
    #No se tienen en cuenta los CUPS para que no devuelva los nodos de terminación de red una vez conectados los CUPS.
    splitting_nodes_sin_cups = []
    #Identificar los nodos finales de la red, tengan CUPS asociados o no.
    end_nodes_sin_cups = []
    cups_grafo = []   
    trafos_grafo = []
    if graph_data_error < 3:
        ind_cups_agregado_CT = 0
        if 'cups_agregado_CT' not in locals():
            cups_agregado_CT = pd.DataFrame(columns=['CUPS', 'TRAFO', 'CUPS_X', 'CUPS_Y'])
            ind_cups_agregado_CT = 1
    
        for node, data in G.nodes(data=True):
            if data['TR'] != 'CT' and data['TR'] not in trafos_grafo:
                trafos_grafo.append(str(data['TR']))
                    
            if data['Tipo_Nodo'] == 'CUPS' or data['Tipo_Nodo'] == 'CUPS_TR':
                # cups_grafo += [node]
                cups_grafo.append(node)
            else:
                # if G.nodes[node]['Tipo_Nodo'] != 'CT' and G.nodes[node]['Tipo_Nodo'] != 'CT_Virtual':
                a = list(np.unique(list(G.edges(node))))
                a.remove(node)
                num_cups = 0
                num_desc = 0
                num_desc_spl = 0
                
                #Los nodos CUP no tienen asignados estos atributos.
                # if data['Tipo_Nodo'] != 'CUPS' and data['Tipo_Nodo'] != 'CUPS_TR':
                    #Se crea un campo que define el número de enlaces que tiene cada nodo y otro que sirve como indicador de la agregación aguas abajo para más adelante
                    #Es necesario para la resolución del grafo en caso de lazos.
                    #CUIDADO, no contar los enlaces con los CUPS en estos dos atributos
                    # G.nodes[node]['Enlaces_orig1'] = 0
                    # G.nodes[node]['Enlaces_iter1'] = 0
                    
                for row in a:
                    #Para obtener nodos_cups_conectados y nodos_cups_descendientes
                    if data['Tipo_Nodo'] != 'CT' and data['Tipo_Nodo'] != 'CT_Virtual':
                        if G.nodes[row]['Tipo_Nodo'] == 'CUPS' or G.nodes[row]['Tipo_Nodo'] == 'CUPS_TR':
                            num_cups += 1
                        else:
                            num_desc +=1
                            
                    #Para splitting_nodes_sin_cups
                    if G.nodes[row]['Tipo_Nodo'] != 'CUPS' and data['Tipo_Nodo'] != 'CUPS' and G.nodes[row]['Tipo_Nodo'] != 'CUPS_TR' and data['Tipo_Nodo'] != 'CUPS_TR':
                        # G.nodes[node]['Enlaces_orig1'] = G.nodes[node]['Enlaces_orig1'] + 1
                        # G.nodes[node]['Enlaces_iter1'] = G.nodes[node]['Enlaces_iter1'] + 1
                        num_desc_spl += 1
                    
                    if ind_cups_agregado_CT == 1 and (data['Tipo_Nodo'] == 'CT' or data['Tipo_Nodo'] == 'CT_Virtual'):
                        # if G.nodes[row]['Tipo_Nodo'] == 'CUPS' or G.nodes[row]['Tipo_Nodo'] == 'CUPS_TR':
                        if G.nodes[row]['Tipo_Nodo'] == 'CUPS_TR':
                            #Se han visto dos CUPS para una misma salida del trafo, ambos con el mismo ID_CT pero uno era TRAFGISS03733T12 y otro TRAFGISS09615T12 (TORRE, 3733)
                            if row.find(str(self.id_ct)) >= 0:
                                cups_agregado_CT = cups_agregado_CT.append({'CUPS': str(row), 'TRAFO': G.nodes[row]['TR'], 'CUPS_X': G.nodes[row]['pos'][0], 'CUPS_Y': G.nodes[row]['pos'][1]}, ignore_index=True)
                            else:
                                f123 = open(self.ruta_raiz + "cups_repetidos_trafo.txt", 'a')
                                f123.write(str(self.Nombre_CT) + ',' + str(self.id_ct) + ',' + str(row) + '\n')
                                
                                cups_agregado_CT = cups_agregado_CT.append({'CUPS': str(row), 'TRAFO': G.nodes[row]['TR'], 'CUPS_X': G.nodes[row]['pos'][0], 'CUPS_Y': G.nodes[row]['pos'][1]}, ignore_index=True)
                                print('Error. Encontrado el CUPS ' + str(row) + ' en un trafo y NO se corresponde con el ID_CT: ' + str(self.id_ct) + '. CUPS ignorado.')
                                print(cups_agregado_CT)
                                # import time
                                # time.sleep(5)
                                logger.error('Error. Encontrado el CUPS ' + str(row) + ' en un trafo y NO se corresponde con el ID_CT: ' + str(self.id_ct) + '. CUPS ignorado.')
                
                if num_desc_spl >= 3:
                    splitting_nodes_sin_cups += [node]
                        
                if num_cups > 0 and num_desc == 1:
                    nodos_cups_conectados.append(node)
                if num_cups > 0 and num_desc > 1:
                    nodos_cups_descendientes.append(node)
                
                # if data['Tipo_Nodo'] != 'CUPS' and data['Tipo_Nodo'] != 'CUPS_TR' and num_desc == 1:
                if num_desc == 1 and num_cups == 0:
                    # end_nodes_sin_cups += [node]
                    end_nodes_sin_cups.append(node)
                    
                # if data['Tipo_Nodo'] == 'CUPS' or data['Tipo_Nodo'] == 'CUPS_TR':
                #     cups_grafo += [node]
                    
                # if data['Tipo_Nodo'] == 'CT' and data['Tipo_Nodo'] == 'CT_Virtual':
        
        #Se eliminan duplicados
        nodos_cups_conectados = list(dict.fromkeys(nodos_cups_conectados))
        nodos_cups_descendientes = list(dict.fromkeys(nodos_cups_descendientes))
        splitting_nodes_sin_cups = list(dict.fromkeys(splitting_nodes_sin_cups))
        end_nodes_sin_cups = list(dict.fromkeys(end_nodes_sin_cups))
               
    
        derivation_nodes = [x for x,y in G.nodes(data=True) if y['Tipo_Nodo']=='DERIVACION']
            
        logger.debug('Encontrados ' + str(len(splitting_nodes_sin_cups)) + ' nodos con bifurcaciones, de los cuales ' + str(len(derivation_nodes)) + ' se consideran derivación; y ' + str(len(end_nodes_sin_cups)) + ' nodos terminación de línea.')

    
    
    #ELIMINAR
    print(cups_agregado_CT)
    return
    # time.sleep(3)
    
    
    ##############################################################################
    ## Lectura de las curvas de carga
    ##############################################################################
    #Adaptación de la fecha para crear un ID para el SQL
    fecha_datetime = self.fecha_ini
    fecha = int(str(fecha_datetime.strftime("%Y")) + str(fecha_datetime.strftime("%m")) + str(fecha_datetime.strftime("%d")))
    
    #Se leen las curvas de carga del mes correspondiente al primer día. Después se actualizará si se cambia de mes.
    df_cch, df_cch_AE_giss, df_cch_AS_giss = self.get_cch_cups(fecha, self.ruta_cch, cups_grafo)
    

    while fecha_datetime < self.fecha_fin + datetime.timedelta(days=1):
        fecha = int(str(fecha_datetime.strftime("%Y")) + str(fecha_datetime.strftime("%m")) + str(fecha_datetime.strftime("%d")))
        
        #Se comprueba si al pasar al día siguiente se cambia de mes y se extraen los datos de todo ese mes si es necesario.
        if fecha_datetime > self.fecha_ini and (fecha_datetime - datetime.timedelta(days=1)).month != fecha_datetime.month:
            del df_cch, df_cch_AE_giss, df_cch_AS_giss
            df_cch, df_cch_AE_giss, df_cch_AS_giss = self.get_cch_cups(fecha, self.ruta_cch, cups_grafo)

        ##############################################################################  
        ## Filtrado de las curvas de carga.
        ## Se extraen valores de potencia entregada a los clientes. Magnitud 7, AE
        ## Se extraen valores de potencia suministrada (autoconsumo) por los clientes. Magnitud 8, AS
        ## En ambos casos se separa según data_validation (validez del tipo de dato.)
        ##############################################################################
        #Potencia entregada. AE
        #DF con valores de potencia entregada a los clientes (7) con data_validation = A (valores válidos)
        df_AE_7A = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'A')]
        df_AE = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'A')]
        #DF con valores de potencia entregada a los clientes (7) con data_validation = P (valores parcialmente válidos)
        df_AE_7P = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'P')]
        # df_AE = df_AE.append(df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'P')], ignore_index=True).reset_index(drop=True)
        #Se añade al DF original.
        df_AE = df_AE.append(df_AE_7P, ignore_index=True).reset_index(drop=True)
        #DF con valores de potencia entregada a los clientes (7) con data_validation = N (valores inválidos)
        #DECIDIR SI CONSIDERARLOS O NO.
        df_AE_7N = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'N')]
        #Se añade al DF original.
        df_AE = df_AE.append(df_AE_7N, ignore_index=True).reset_index(drop=True)
        
        logger.debug('Registros AE clientes encontrados: 7A=' + str(len(df_AE_7A)) + ', 7P=' + str(len(df_AE_7P)) + ', 7N=' +str(len(df_AE_7N)) + '. Duplicados encontrados: ' + str(len(df_AE)-len(df_AE.drop_duplicates())))
        #Se eliminan duplicados
        df_AE = df_AE.drop_duplicates(keep = 'first').reset_index(drop=True)
        
        df_AE_fecha = df_AE[df_AE['FECHA'] == fecha].reset_index(drop=True)
        
        
        #Potencia suministrada (autoconsumo). AS
        #DF con valores de potencia entregada a los clientes (7) con data_validation = A (valores válidos)
        df_AS_8A = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 8) & (df_cch['DATA_VALIDATION'] == 'A')]
        df_AS = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 8) & (df_cch['DATA_VALIDATION'] == 'A')]
        #DF con valores de potencia entregada a los clientes (7) con data_validation = P (valores parcialmente válidos)
        df_AS_8P = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 8) & (df_cch['DATA_VALIDATION'] == 'P')]
        # df_AS = df_AS.append(df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 8) & (df_cch['DATA_VALIDATION'] == 'P')], ignore_index=True).reset_index(drop=True)
        #Se añade al DF original.
        df_AS = df_AS.append(df_AS_8P, ignore_index=True).reset_index(drop=True)
        #DF con valores de potencia entregada a los clientes (7) con data_validation = N (valores inválidos)
        #No se van a considerar estos valores.
        df_AS_8N = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 8) & (df_cch['DATA_VALIDATION'] == 'N')]
        #Se añade al DF original.
        df_AS = df_AS.append(df_AS_8N, ignore_index=True).reset_index(drop=True)
        
        logger.debug('Registros AS clientes encontrados: 8A=' + str(len(df_AS_8A)) + ', 8P=' + str(len(df_AS_8P)) + ', 8N=' +str(len(df_AS_8N)) + '. Duplicados encontrados: ' + str(len(df_AS)-len(df_AS.drop_duplicates())))
        #Se eliminan duplicados
        df_AS = df_AS.drop_duplicates(keep = 'first').reset_index(drop=True)
        
        df_AS_fecha = df_AS[df_AS['FECHA'] == fecha].reset_index(drop=True)
        
        
        ##TEMPORAL
        #Se guarda la info en dos archivos txt
        # f_temp = open ("F:\GTEA\DEPERTEC\Grafo\AE_DEPERTEC.txt", "a")
        # f_temp.write(str(self.id_ct) + ";" + str(self.Nombre_CT) + ';' + str(fecha_datetime.year) + ';' + str(fecha_datetime.month) + ';' + str(len(df_AE_7A)) + ';' + str(len(df_AE_7P)) + ';' + str(len(df_AE_7N)) + ';' + str(len(df_cch_AE_giss)) + "\n")
        # f_temp.close()
        # f_temp = open ("F:\GTEA\DEPERTEC\Grafo\AS_DEPERTEC.txt", "a")
        # f_temp.write(str(self.id_ct) + ";" + str(self.Nombre_CT) + ';' + str(fecha_datetime.year) + ';' + str(fecha_datetime.month) + ';' + str(len(df_AS_8A)) + ';' + str(len(df_AS_8P)) + ';' + str(len(df_AS_8N)) + ';' + str(len(df_cch_AS_giss)) + "\n")
        # f_temp.close()
        del df_AE_7A, df_AE_7P, df_AE_7N, df_AS_8A, df_AS_8P, df_AS_8N
        # fecha_datetime = fecha_datetime + datetime.timedelta(days=1)
        # continue
        
        
        #Se comprueba si no hay ningún valor de CCH de clientes, porque si es 0, aunque haya valores en el CT no se pueden calcular pérdidas.
        if len(df_AE_fecha) == 0 and len(df_AS_fecha) == 0:
            logger.error('Error al cargar las curvas de carga para la fecha: ' + str(fecha) + '. df_AE_fecha y df_AS_fecha == 0.')
            fecha_datetime = fecha_datetime + datetime.timedelta(days=1)
            continue
        else:
            #Obtención de los CUPS únicos de la curva de carga
            CUPS_unicos = df_AE_fecha.drop_duplicates(subset=['CUPS'], keep='last')['CUPS'].reset_index(drop=True)
            
            logger.debug('Encontrados ' + str(len(CUPS_unicos)) + ' CUPS únicos en los archivos de curvas de carga de clientes.')
            
            #Se recorre el diccionario de horas para aplicar sobre el grafo los valores de potencia de cada hora por separado y hacer los cálculos.
            for colum_hora in diccionario_horas.keys():
                # colum_hora = clave     
                
                #Función para agregar al grafo las curvas de carga de leídas.
                G = self.add_cch_grafo(G, colum_hora, df_AE_fecha, df_AS_fecha)#, cups_agregado_CT, self.id_ct)                
                
                #Se crea una lista con todos los nodos que pueden ser terminación de línea y/o tener CUPs conectados
                end_nodes_cups = []
                end_nodes_cups = end_nodes_sin_cups + nodos_cups_conectados
                end_nodes_cups = list(dict.fromkeys(end_nodes_cups))
                #Posible caso del id_ct en la lista end_nodes. Hay que eliminarlo. Caso donde solo salga una línea del CT
                if self.id_ct in end_nodes_cups:
                    end_nodes_cups.remove(self.id_ct)
                
                #Se elimina el id_ct de splitting nodes para no iterar sobre él.
                #No tendría que ser necesario eliminarlo
                if self.id_ct in splitting_nodes_sin_cups:
                    splitting_nodes_sin_cups.remove(self.id_ct)
                
                #Parámetro para evaluar si el resultado numérico obtenido es adecuado.
                CCH_Data_Error = 3
                        
                #Se resuelve el grafo
                G, CCH_Data_Error = self.resuelve_grafo(G, end_nodes_cups, self.id_ct, splitting_nodes_sin_cups, nodos_cups_descendientes, self.temp_cables, CCH_Data_Error)
                    
                #Si es 4 ha habido un error de resolución del grafo por entrar en bucles irresolubles.
                if CCH_Data_Error == 4:
                    logger.error('CCH_Data_Error = ' + str(CCH_Data_Error) + '. Se aborta la resolución del grafo y no se guardan valores para colum_hora=' + str(colum_hora))
                    break
                
                
                ##############################################################################
                ## Conexión con la BBDD SQL.
                ##############################################################################
                #Se define el nombre de dos de las tablas SQL, las que contendrán todos los datos del grafo
                tabla_ct_nodos = "OUTPUT_" + str(self.id_ct) + "_" + str(self.Nombre_CT).replace(' ','_') + "_NODOS"
                tabla_ct_trazas = "OUTPUT_" + str(self.id_ct) + "_" + str(self.Nombre_CT).replace(' ','_') + "_TRAZAS" 
                
                if (self.save_ddbb == 0) or (self.save_ddbb == 1) or (self.save_ddbb == 2):
                    try:
                        conn = pyodbc.connect('Driver={SQL Server};'
                                              'Server=' + ip_server + ';'
                                              'Database=' + db_server + ';'
                                              'UID=' + usr_server + ';'
                                              'PWD=' + pwd_server)
                                             #'Trusted_Connection=yes;')
                        cursor = conn.cursor()
                    except:
                        logger.error('Error de conexión con la BBDD. Ejecución abortada.')
                        raise
                
                
                ##############################################################################
                ## Obtención de los parámetros finales para el escenario definido y guardado de datos.
                ##############################################################################
                
                logger.debug('Cálculo realizado para: ' + self.Nombre_CT + ' ' + str(fecha) + ' ' + colum_hora)
                
                #Importante el .zfill(5), es necesario que el número tenga los 0 delante necesarios para no ser confundido con otro CT que contenga número similares. (Ej. 00832 y 08323)
                AE_medida_ct = df_cch_AE_giss.loc[(df_cch_AE_giss['CODIGO_LVC'].str.find(str(self.id_ct).zfill(5)) >= 0) & (df_cch_AE_giss['FECHA'] == fecha)].reset_index(drop=True)
                AS_medida_ct = df_cch_AS_giss.loc[(df_cch_AS_giss['CODIGO_LVC'].str.find(str(self.id_ct).zfill(5)) >= 0) & (df_cch_AS_giss['FECHA'] == fecha)].reset_index(drop=True)
                
                #Método para obtener los resultados requeridos según CT, TR y nivel de tensión (05-2021)
                lista_nodos_resultados = [str(self.id_ct)]
                lista_temp = list(np.unique(list(G.edges(str(self.id_ct)))))
                lista_temp.remove(str(self.id_ct))
                lista_nodos_resultados = lista_nodos_resultados + lista_temp
                for i in lista_temp:
                    lista_temp2 = list(np.unique(list(G.edges(i))))
                    lista_temp2.remove(i)
                    lista_temp2.remove(str(self.id_ct))
                    for j in lista_temp2:
                        if G.nodes[j]['Tipo_Nodo'] != 'CUPS_TR':
                            lista_nodos_resultados.append(j)
                del lista_temp, lista_temp2
                
                #Se recorren los nodos de la lista hallada para ir calculando las cargas conectadas, pérdidas y el medido en el CT aguas abajo de cada uno de ellos.
                for row in lista_nodos_resultados:
                    # Pérdidas totales en las trazas
                    AE_R_vanos_tot = 0
                    Q_R_vanos_tot = 0
                    AE_S_vanos_tot = 0
                    Q_S_vanos_tot = 0
                    AE_T_vanos_tot = 0
                    Q_T_vanos_tot = 0
                    
                    AS_R_vanos_tot = 0
                    AS_S_vanos_tot = 0
                    AS_T_vanos_tot = 0
                    
                    # Se calcula el agregado total de las curvas de carga
                    P_R_carga_tot = 0
                    Q_R_carga_tot = 0
                    P_S_carga_tot = 0
                    Q_S_carga_tot = 0
                    P_T_carga_tot = 0
                    Q_T_carga_tot = 0
                    
                    P_R_CT_tot = G.nodes[str(row)]['P_R_0']
                    Q_R_CT_tot = G.nodes[str(row)]['Q_R_0']
                    P_S_CT_tot = G.nodes[str(row)]['P_S_0']
                    Q_S_CT_tot = G.nodes[str(row)]['Q_S_0']
                    P_T_CT_tot = G.nodes[str(row)]['P_T_0']
                    Q_T_CT_tot = G.nodes[str(row)]['Q_T_0']
                    
                    #Pérdidas medidas en el CT/trafo/nivel de tensión
                    AE_cch_ct = 0
                    AS_cch_ct = 0
            
                    #Si es un nodo de salida de tensión de trafo
                    if str(row).find('_230') >= 0 or str(row).find('_400') >= 0:
                        #Se localiza el valor medido en el CT.
                        if int(G.nodes[row]['QBT_TENSION']) == 230:
                            try:
                                AE_cch_ct = AE_medida_ct.loc[AE_medida_ct.CODIGO_LVC.str.find(str(G.nodes[row]['TR'].replace('R','') + '1')) >= 0][colum_hora].sum()
                                if AE_cch_ct > 0:
                                    aa = 'Todo ok'
                                    del aa
                            except:
                                AE_cch_ct = 0
                            try:
                                AS_cch_ct = AS_medida_ct.loc[AS_medida_ct.CODIGO_LVC.str.find(str(G.nodes[row]['TR'].replace('R','') + '1')) >= 0][colum_hora].sum()
                                if AS_cch_ct > 0:
                                    aa = 'Todo ok'
                                    del aa
                            except:
                                AS_cch_ct = 0
                                
                            # codigo_LVC = AE_medida_ct.loc[AE_medida_ct.CODIGO_LVC.str.find(str(G.nodes[row]['TR'].replace('R','') + '1')) >= 0]['CODIGO_LVC'][0]
                        elif int(G.nodes[row]['QBT_TENSION']) == 400:
                            try:
                                AE_cch_ct = AE_medida_ct.loc[AE_medida_ct.CODIGO_LVC.str.find(str(G.nodes[row]['TR'].replace('R','') + '2')) >= 0][colum_hora].sum()
                                if AE_cch_ct > 0:
                                    aa = 'Todo ok'
                                    del aa
                            except:
                                AE_cch_ct = 0
                            try:
                                AS_cch_ct = AS_medida_ct.loc[AS_medida_ct.CODIGO_LVC.str.find(str(G.nodes[row]['TR'].replace('R','') + '2')) >= 0][colum_hora].sum()
                                if AS_cch_ct > 0:
                                    aa = 'Todo ok'
                                    del aa
                            except:
                                AS_cch_ct = 0
                                
                            # codigo_LVC = AE_medida_ct.loc[AE_medida_ct.CODIGO_LVC.str.find(str(G.nodes[row]['TR'].replace('R','') + '1')) >= 0]['CODIGO_LVC'][0]
                            
                        
                        
                        #Para obtener todas las pérdidas asociadas:
                        for nodo1, nodo2, keys, data in G.edges(data = True, default = 0, keys=True): 
                            #Se filtran para considerar solo las trazas con TR y QBT_TENSION oportuno
                            if data['TR'] == G.nodes[row]['TR'] and data['QBT_TENSION'] == G.nodes[row]['QBT_TENSION']:
                                #Se suman las pérdidas de todos los vanos asociados
                                #Hay que considerar que desde la última arqueta hasta el CUPS solo está definida la P y Q de la fase correspondiente, sin los try da error.
                                try:
                                    if G.edges[nodo1, nodo2, keys]['P_R_Linea'] >= 0:
                                        AE_R_vanos_tot += G.edges[nodo1, nodo2, keys]['P_R_Linea']
                                        Q_R_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_R_Linea']
                                    else:
                                        AS_R_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_R_Linea'])
                                except:
                                    pass
                                try:
                                    if G.edges[nodo1, nodo2, keys]['P_S_Linea'] >= 0:
                                        AE_S_vanos_tot += G.edges[nodo1, nodo2, keys]['P_S_Linea']
                                        Q_S_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_S_Linea']
                                    else:
                                        AS_S_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_S_Linea'])
                                except:
                                    pass
                                try:
                                    if G.edges[nodo1, nodo2, keys]['P_T_Linea'] >= 0:
                                        AE_T_vanos_tot += G.edges[nodo1, nodo2, keys]['P_T_Linea']
                                        Q_T_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_T_Linea']
                                    else:
                                        AS_T_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_T_Linea'])
                                except:
                                    pass
                                
                        #Para obtener la carga conectada:
                        for nodo, data in G.nodes(data = True, default = 0):
                            #Se filtran para considerar solo nodos con TR y QBT_TENSION oportuno y que solo sean CUPS.
                            if data['TR'] ==  G.nodes[row]['TR'] and data['QBT_TENSION'] == G.nodes[row]['QBT_TENSION'] and data['Tipo_Nodo'] == 'CUPS':
                                P_R_carga_tot += data['P_R_0']
                                Q_R_carga_tot += data['Q_R_0']
                                P_S_carga_tot += data['P_S_0']
                                Q_S_carga_tot += data['Q_S_0']
                                P_T_carga_tot += data['P_T_0']
                                Q_T_carga_tot += data['Q_T_0']
                        
                        codigo_LVC = row
                                
                    #Si es el CT con el agregado total
                    elif G.nodes[str(row)]['TR'] == 'CT':
                        try:
                            AE_cch_ct =  AE_medida_ct[colum_hora].sum()
                            if AE_cch_ct > 0:
                                aa = 'Todo ok'
                                del aa
                        except:
                            AE_cch_ct = 0
                        try:
                            AS_cch_ct = AS_medida_ct[colum_hora].sum()
                            if AS_cch_ct > 0:
                                aa = 'Todo ok'
                                del aa
                        except:
                            AS_cch_ct = 0
                                
                        # codigo_LVC = self.id_ct
                        #Para obtener todas las pérdidas asociadas:
                        for nodo1, nodo2, keys, data in G.edges(data = True, default = 0, keys=True): 
                            #No hay que aplicar filtro, se quieren todas las pérdidas del grafo.
                            #Hay que considerar que desde la última arqueta hasta el CUPS solo está definida la P y Q de la fase correspondiente, sin los try da error.
                            try:
                                if G.edges[nodo1, nodo2, keys]['P_R_Linea'] >= 0:
                                    AE_R_vanos_tot += G.edges[nodo1, nodo2, keys]['P_R_Linea']
                                    Q_R_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_R_Linea']
                                else:
                                    AS_R_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_R_Linea'])
                            except:
                                pass
                            try:
                                if G.edges[nodo1, nodo2, keys]['P_S_Linea'] >= 0:
                                    AE_S_vanos_tot += G.edges[nodo1, nodo2, keys]['P_S_Linea']
                                    Q_S_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_S_Linea']
                                else:
                                    AS_S_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_S_Linea'])
                            except:
                                pass
                            try:
                                if G.edges[nodo1, nodo2, keys]['P_T_Linea'] >= 0:
                                    AE_T_vanos_tot += G.edges[nodo1, nodo2, keys]['P_T_Linea']
                                    Q_T_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_T_Linea']
                                else:
                                    AS_T_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_T_Linea'])
                            except:
                                pass
                            
                        #Para obtener la carga conectada:
                        for nodo, data in G.nodes(data = True, default = 0):
                            #Se filtran solo para considerar los CUPS
                            if data['Tipo_Nodo'] == 'CUPS':
                                P_R_carga_tot += data['P_R_0']
                                Q_R_carga_tot += data['Q_R_0']
                                P_S_carga_tot += data['P_S_0']
                                Q_S_carga_tot += data['Q_S_0']
                                P_T_carga_tot += data['P_T_0']
                                Q_T_carga_tot += data['Q_T_0']   
                                
                        codigo_LVC = 'CT'    
                    
                    #Si es un nodo que representa a un trafo (ni tendrá _230 o _400 ni TR=='CT')
                    else:
                        try:
                            AE_cch_ct = AE_medida_ct.loc[AE_medida_ct.CODIGO_LVC.str.find(str(row).replace('_TR','T')) >= 0][colum_hora].sum()
                            if AE_cch_ct > 0:
                                aa = 'Todo ok'
                                del aa
                        except:
                            AE_cch_ct = 0
                        try:
                            AS_cch_ct = AS_medida_ct.loc[AS_medida_ct.CODIGO_LVC.str.find(str(row).replace('_TR','T')) >= 0][colum_hora].sum()
                            if AS_cch_ct > 0:
                                aa = 'Todo ok'
                                del aa
                        except:
                            AS_cch_ct = 0
                            
                        # codigo_LVC = row
                        #Para obtener todas las pérdidas asociadas:
                        for nodo1, nodo2, keys, data in G.edges(data = True, default = 0, keys=True):
                            #Se filtran para considerar solo las trazas de este trafo, pero obviando los niveles de tensión.
                            if data['TR'] == G.nodes[row]['TR']:
                                #Hay que considerar que desde la última arqueta hasta el CUPS solo está definida la P y Q de la fase correspondiente, sin los try da error.
                                try:
                                    if G.edges[nodo1, nodo2, keys]['P_R_Linea'] >= 0:
                                        AE_R_vanos_tot += G.edges[nodo1, nodo2, keys]['P_R_Linea']
                                        Q_R_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_R_Linea']
                                    else:
                                        AS_R_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_R_Linea'])
                                except:
                                    pass
                                try:
                                    if G.edges[nodo1, nodo2, keys]['P_S_Linea'] >= 0:
                                        AE_S_vanos_tot += G.edges[nodo1, nodo2, keys]['P_S_Linea']
                                        Q_S_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_S_Linea']
                                    else:
                                        AS_S_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_S_Linea'])
                                except:
                                    pass
                                try:
                                    if G.edges[nodo1, nodo2, keys]['P_T_Linea'] >= 0:
                                        AE_T_vanos_tot += G.edges[nodo1, nodo2, keys]['P_T_Linea']
                                        Q_T_vanos_tot += G.edges[nodo1, nodo2, keys]['Q_T_Linea']
                                    else:
                                        AS_T_vanos_tot += abs(G.edges[nodo1, nodo2, keys]['P_T_Linea'])
                                except:
                                    pass
                                
                        #Para obtener la carga conectada:
                        for nodo, data in G.nodes(data = True, default = 0):
                            #Se consideran solo los nodos de ese TR que son CUPS, obviando el QBT_TENSION.
                            if data['TR'] ==  G.nodes[row]['TR']  and data['Tipo_Nodo'] == 'CUPS':
                                P_R_carga_tot += data['P_R_0']
                                Q_R_carga_tot += data['Q_R_0']
                                P_S_carga_tot += data['P_S_0']
                                Q_S_carga_tot += data['Q_S_0']
                                P_T_carga_tot += data['P_T_0']
                                Q_T_carga_tot += data['Q_T_0']
                                
                        codigo_LVC = row #'TRAFO'
                        
                        
                    if P_R_carga_tot == 0 or P_S_carga_tot == 0 or P_T_carga_tot == 0:
                        logger.warning('La potencia total agregada en los CUPS es es 0 en alguna de las fases (R, S, T): ' + str(P_R_carga_tot) + ', ' + str(P_S_carga_tot) + ', ' + str(P_T_carga_tot))
                    
                    #Se comprueba que el valor medido en el CT no es 0 y que hay cargas conectadas. Si no hay cargas y el medido es 0 significa que es un trafo solo con salida de 230 pero que esta es la salida de 400 creada inicialmente y que hay que despreciar.
                    if AE_cch_ct == 0 and P_R_carga_tot == 0 and P_S_carga_tot == 0 and P_T_carga_tot == 0:
                        print('Error, algo es 0.')
                        logger.error('AE_cch_ct, P_R_carga_tot, P_S_carga_tot, P_Q_carga_tot son 0. Se aborta la resolución del grafo.')
                        fecha_datetime = fecha_datetime + datetime.timedelta(days=1)
                        continue
                    elif AE_cch_ct == 0:
                        CCH_Data_Error = 3 #Otros casos (valor medido = 0)
                    else:
                        if -self.upper_limit <= (100 - ((P_R_CT_tot + P_S_CT_tot + P_T_CT_tot)*100 / AE_cch_ct)) <= 0:
                            CCH_Data_Error = 0 #Si el calculado está entre el 100 y el 110% del medido
                        elif 0 <= (100 - ((P_R_CT_tot + P_S_CT_tot + P_T_CT_tot)*100 / AE_cch_ct)) <= self.lower_limit:
                            CCH_Data_Error = 0 #Si el calculado está entre el 90 y el 100% del medido
                        elif (100 - ((P_R_CT_tot + P_S_CT_tot + P_T_CT_tot)*100 / AE_cch_ct)) > self.lower_limit:
                            CCH_Data_Error = 1 #Si el calculado es inferior al 90% del medido
                        elif (100 - ((P_R_CT_tot + P_S_CT_tot + P_T_CT_tot)*100 / AE_cch_ct)) < -self.upper_limit:
                            CCH_Data_Error = 2 #Si el calculado es superior al 110% del medido
                        else:
                            CCH_Data_Error = 3 #Otros casos
                            
                      
                    #Al llegar a la hora 24 ya estamos en el día siguiente y se suma 1 a la fecha para guardar correctamente el valor.
                    #Identificador del caso para el SQL
                    id_caso = int(str(fecha) + str(diccionario_horas.get(colum_hora)))
                    fecha_sql = fecha
                    if colum_hora == 'VALOR_H24':
                    #     fecha_datetime = fecha_datetime + datetime.timedelta(days=1)
                    #     fecha = int(str(fecha_datetime.strftime("%Y")) + str(fecha_datetime.strftime("%m")) + str(fecha_datetime.strftime("%d")))
                        fecha_sql = int(str((fecha_datetime + datetime.timedelta(days=1)).strftime("%Y")) + str((fecha_datetime + datetime.timedelta(days=1)).strftime("%m")) + str((fecha_datetime + datetime.timedelta(days=1)).strftime("%d")))
                        # id_caso = int(str(int(str((fecha_datetime + datetime.timedelta(days=1)).strftime("%Y")) + str((fecha_datetime + datetime.timedelta(days=1)).strftime("%m")) + str((fecha_datetime + datetime.timedelta(days=1)).strftime("%d")))) + str(diccionario_horas.get(colum_hora)))
                        id_caso = int(str(fecha_sql) + str(diccionario_horas.get(colum_hora)))

                        
                    print('\n' + self.Nombre_CT + ' ' + str(fecha) + ' ' + colum_hora)
                    print('id_caso ' + str(id_caso) + ' ID trafo: ' + str(row))
                    print('Total pérdidas vanos (R, S, T): ', AE_R_vanos_tot, AE_S_vanos_tot, AE_T_vanos_tot, ' kW (' + str(AE_R_vanos_tot + AE_S_vanos_tot + AE_T_vanos_tot) + '), ', Q_R_vanos_tot, Q_S_vanos_tot, Q_T_vanos_tot, 'kVAr')
                    print('Total AE MEDIDO en el CT - ' + str(row) + ' (kW): ' + str(AE_cch_ct))
                    print('Total AS MEDIDO en el CT - ' + str(row) + ' (kW): ' + str(AS_cch_ct))
                    print('Total CALCULADO en el CT (curvas de carga + pérdidas) ' + str(row) + ' (kW): ' + str(P_R_CT_tot + P_S_CT_tot + P_T_CT_tot))
                    print(str(G.nodes[str(row)]['P_R_0']), str(G.nodes[str(row)]['P_S_0']), str(G.nodes[str(row)]['P_T_0']))
                    print('Total cargas conectadas (R, S, T): ', P_R_carga_tot, P_S_carga_tot, P_T_carga_tot, ' kW (' +  str(P_R_carga_tot + P_S_carga_tot + P_T_carga_tot) + '), ', Q_R_carga_tot, Q_S_carga_tot, Q_T_carga_tot, 'kVAR')
                    print('Suma total curvas de carga clientes: ' + str(df_AE_fecha[colum_hora].sum()))
                    print('CCH_Data_Error: ' + str(CCH_Data_Error))
                    print('CCH + pérdidas: ' + str(P_R_carga_tot + P_S_carga_tot + P_T_carga_tot + AE_R_vanos_tot + AE_S_vanos_tot + AE_T_vanos_tot))
                

                
                    ##############################################################################
                    ## Guardado de datos en la BBDD SQL.
                    ##############################################################################
                    if (self.save_ddbb == 0) or (self.save_ddbb == 1):
                        #Se comprueba que exista la tabla en la BBDD
                        SQL_Query = pd.read_sql_query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '" + str(self.tabla_cts_general) + "'", conn)
                        df_table_exist = pd.DataFrame(SQL_Query, columns=['TABLE_CATALOG','TABLE_SCHEMA','TABLE_NAME','TABLE_TYPE'])
                
                        if len(df_table_exist) > 0:
                            try:
                                #instruccion_insert = "INSERT INTO " + self.tabla_cts_general + " (ID_Caso, ID_CT, CT_NOMBRE, TRAFO, CODIGO_LVC, CCH_Data_Error, Fecha, Hora, P_R_CT_KW, Q_R_CT_KVAR, P_S_CT_KW, Q_S_CT_KVAR,P_T_CT_KW, Q_T_CT_KVAR, P_CT_MEDIDO_KW, Q_CT_MEDIDO_KVAR, P_R_LINEAS_KW, Q_R_LINEAS_KVAR, P_S_LINEAS_KW, Q_S_LINEAS_KVAR, P_T_LINEAS_KW, Q_T_LINEAS_KVAR) VALUES (" + str(id_caso) + ", " + str(self.id_ct) + ", '" + self.Nombre_CT + "', '" + str(row.TRAFO) + "','"+ str(id_trafo_cch) + "',"  + str(int(CCH_Data_Error)) + ",'" + str(fecha) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(P_R_CT_tot)) + ", " + str(float(Q_R_CT_tot)) + ", " + str(float(P_S_CT_tot)) + ", " + str(float(Q_S_CT_tot)) + ", " + str(float(P_T_CT_tot)) + ", " + str(float(Q_T_CT_tot)) + ", " + str(float(AE_cch_ct)) + ", 0, " + str(float(AE_R_vanos_tot)) + ", " + str(float(Q_R_vanos_tot)) + ", " + str(float(AE_S_vanos_tot)) + ", " + str(float(Q_S_vanos_tot)) + ", " + str(float(AE_T_vanos_tot)) + ", " + str(float(Q_T_vanos_tot)) + ");"
                                # instruccion_insert = "INSERT INTO " + self.tabla_cts_general + " (ID_Caso, ID_CT, CT_NOMBRE, ID_TRAFO, CODIGO_LVC, CCH_Data_Error, Fecha, Hora, P_R_CT_KW, Q_R_CT_KVAR, P_S_CT_KW, Q_S_CT_KVAR,P_T_CT_KW, Q_T_CT_KVAR, P_CT_MEDIDO_KW, Q_CT_MEDIDO_KVAR, P_R_LINEAS_KW, Q_R_LINEAS_KVAR, P_S_LINEAS_KW, Q_S_LINEAS_KVAR, P_T_LINEAS_KW, Q_T_LINEAS_KVAR) VALUES (" + str(id_caso) + ", " + str(self.id_ct) + ", '" + self.Nombre_CT + "', '" + str(row) + "','"+ str(codigo_LVC) + "',"  + str(int(CCH_Data_Error)) + ",'" + str(fecha_sql) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(P_R_CT_tot)) + ", " + str(float(Q_R_CT_tot)) + ", " + str(float(P_S_CT_tot)) + ", " + str(float(Q_S_CT_tot)) + ", " + str(float(P_T_CT_tot)) + ", " + str(float(Q_T_CT_tot)) + ", " + str(float(AE_cch_ct)) + ", 0, " + str(float(AE_R_vanos_tot)) + ", " + str(float(Q_R_vanos_tot)) + ", " + str(float(AE_S_vanos_tot)) + ", " + str(float(Q_S_vanos_tot)) + ", " + str(float(AE_T_vanos_tot)) + ", " + str(float(Q_T_vanos_tot)) + ");"
                                #La reactiva está definida a 0 porque no se ha desarrollado un método de cálculo, aunque el grafo está preparado para asumirlo.
                                #Columnas P_R_CT_KW, P_S_CT_KW, P_T_CT_KW representan el valor CALCULADO de POTENCIA en los nodos del CT (CT, trafo, nivel de tensión). Implica la suma de las CCH de clientes (AE-AS) + pérdidas en la red dependientes del nodo en cuestión (CT, trafo, nivel de tensión)
                                #AE_CT_MEDIDO_KW y AS_CT_MEDIDO_KW son los valores AE y AS medidos en el CT para ese nivel (CT, trafo y nivel de tensión)
                                #AE_R_LINEAS_KW, AE_S_LINEAS_KW, AE_T_LINEAS_KW son las pérdidas totales aguas abajo desde cada nodo del CT (CT, trafo, nivel de tensión) asociadas A LA POTENCIA ETNREGADA POR EL TRAFO, no al posible autoconsumo vertido a la red.
                                #AS_R_LINEAS_KW, AS_S_LINEAS_KW, AS_T_LINEAS_KW son las pérdidas totales aguas abajo desde cada nodo del CT (CT, trafo, nivel de tensión) asociadas AL AUTOCONSUMO, y por lo tanto no aplicables a la potencia vertida por el trafo.
                                instruccion_insert = "INSERT INTO " + self.tabla_cts_general + " (ID_Caso, ID_CT, CT_NOMBRE, ID_TRAFO, CODIGO_LVC, CCH_Data_Error, Fecha, Hora, P_R_CT_KW, P_S_CT_KW, P_T_CT_KW, AE_CT_MEDIDO_KW, AS_CT_MEDIDO_KW, AE_R_LINEAS_KW, AE_S_LINEAS_KW, AE_T_LINEAS_KW, AS_R_LINEAS_KW, AS_S_LINEAS_KW, AS_T_LINEAS_KW) VALUES (" + str(id_caso) + ", " + str(self.id_ct) + ", '" + self.Nombre_CT + "', '" + str(row) + "','"+ str(codigo_LVC) + "',"  + str(int(CCH_Data_Error)) + ",'" + str(fecha_sql) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(P_R_CT_tot)) + ", " + str(float(P_S_CT_tot)) + ", " + str(float(P_T_CT_tot)) + ", " + str(float(AE_cch_ct)) + ", " + str(float(AS_cch_ct)) + ", " + str(float(AE_R_vanos_tot)) + ", " + str(float(AE_S_vanos_tot)) + ", " + str(float(AE_T_vanos_tot)) + ", " + str(float(AS_R_vanos_tot)) + ", " + str(float(AS_S_vanos_tot)) + ", " + str(float(AS_T_vanos_tot)) + ");"
                                # print(instruccion_insert)
                                cursor.execute(instruccion_insert)
                                conn.commit()
                                logger.debug(str(colum_hora) + ' guardado correctamente en la tabla general de la BBDD.')
                            except:
                                logger.error('Error al guardar en la BBDD. ' + instruccion_insert)
                                continue
                        else:
                            logger.error('No existe ninguna tabla con nombre ' + str(self.tabla_cts_general) + ' en la BBDD. Ejecutar en el SQL el comando: ' + "CREATE TABLE [DEPERTEC].[dbo].[" + str(self.tabla_cts_general) + "] (ID_Caso INT, ID_CT INT, CT_NOMBRE VARCHAR(45), ID_TRAFO VARCHAR(15), CODIGO_LVC VARCHAR(15), CCH_Data_Error INT, Fecha DATE, Hora TIME(7), P_R_CT_KW FLOAT, P_S_CT_KW FLOAT, P_T_CT_KW FLOAT, AE_CT_MEDIDO_KW FLOAT, AS_CT_MEDIDO_KW FLOAT, AE_R_LINEAS_KW FLOAT, AE_S_LINEAS_KW FLOAT, AE_T_LINEAS_KW FLOAT, AS_R_LINEAS_KW FLOAT, AS_S_LINEAS_KW FLOAT, AS_T_LINEAS_KW FLOAT);")
                            # instruccion_create = "CREATE TABLE [DEPERTEC].[dbo].[" + str(self.tabla_cts_general) + "] (ID_Caso INT, ID_CT INT, CT_NOMBRE VARCHAR(45), ID_TRAFO VARCHAR(15), CODIGO_LVC VARCHAR(15), CCH_Data_Error INT, Fecha DATE, Hora TIME(7), P_R_CT_KW FLOAT, P_S_CT_KW FLOAT, P_T_CT_KW FLOAT, AE_CT_MEDIDO_KW FLOAT, AS_CT_MEDIDO_KW FLOAT, AE_R_LINEAS_KW FLOAT, AE_S_LINEAS_KW FLOAT, AE_T_LINEAS_KW FLOAT, AS_R_LINEAS_KW FLOAT, AS_S_LINEAS_KW FLOAT, AS_T_LINEAS_KW FLOAT);"
                            #cursor.execute(instruccion_create)
   
                        
                #Se guarda el agregado total de potencia por fase en cada nodo, no se guardan todos los datos para facilitar la gestión de la BBDD.
                #Hay que hacerlo fuera del ciclo de lista_nodos_resultados para que no lo guarde varias veces.
                if (self.save_ddbb == 0) or (self.save_ddbb == 2):
                    #Se comprueba que existe la tabla de nodos y de trazas e la BBDD
                    SQL_Query = pd.read_sql_query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '" + str(tabla_ct_nodos) + "'", conn)
                    df_table_exist = pd.DataFrame(SQL_Query, columns=['TABLE_CATALOG','TABLE_SCHEMA','TABLE_NAME','TABLE_TYPE'])
                    
                    if len(df_table_exist) > 0:
                        #Se recorren todos los nodos y se guardan en el SQL las P y Q calculadas.
                        for nodo, data in G.nodes(data=True, default = 0):                                
                            #Se guarda la información de los nodos, sin contar los CUPS para no guardar demasiados datos.
                            if (data['Tipo_Nodo'] != 'CUPS' and data['Tipo_Nodo'] != 'CUPS_TR'):
                                try:
                                    # instruccion_insert = "INSERT INTO " + tabla_ct_nodos + " (ID_Caso, ID_NODO_LBT_ID, Fecha, Hora, P_R_KW, Q_R_KVAR, P_S_KW, Q_S_KVAR, P_T_KW, Q_T_KVAR) VALUES (" + str(id_caso) + ", '" + str(nodo) + "', '" + str(fecha) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(data_P_R) + ", " + str(data_Q_R) + ", " + str(data_P_S) + ", " + str(data_Q_S) + ", " + str(data_P_T) + ", " + str(data_Q_T) + ");"
                                    instruccion_insert = "INSERT INTO " + tabla_ct_nodos + " (ID_Caso, ID_NODO_LBT_ID, Fecha, Hora, P_R_KW, P_S_KW, P_T_KW) VALUES (" + str(id_caso) + ", '" + str(nodo) + "', '" + str(fecha_sql) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(data['P_R_0']) + ", " + str(data['P_S_0']) + ", " + str(data['P_T_0']) + ");"
                                    cursor.execute(instruccion_insert)
                                    conn.commit()
                                    logger.debug(str(colum_hora) + ' guardado correctamente en la tabla de nodos de la BBDD.')
                                except:
                                    logger.error('Error al guardar en la BBDD. ' + instruccion_insert)
                                    continue
                    else:
                        # logger.error('No existe ninguna tabla con nombre ' + tabla_ct_nodos + ' en la BBDD. Ejecutar en el SQL el comando: ' + "CREATE TABLE [DEPERTEC].[dbo].[" + str(tabla_ct_nodos) + "] (ID_Caso INT, ID_NODO_LBT_ID VARCHAR(45), Fecha DATE, Hora TIME(7), P_R_KW FLOAT, Q_R_KVAR FLOAT, P_S_KW FLOAT, Q_S_KVAR FLOAT, P_T_KW FLOAT, Q_T_KVAR FLOAT);")
                        logger.error('No existe ninguna tabla con nombre ' + str(tabla_ct_nodos) + ' en la BBDD. Ejecutar en el SQL el comando: ' + "CREATE TABLE [DEPERTEC].[dbo].[" + str(tabla_ct_nodos) + "] (ID_Caso INT, ID_NODO_LBT_ID VARCHAR(45), Fecha DATE, Hora TIME(7), P_R_KW FLOAT, P_S_KW FLOAT, P_T_KW FLOAT);")
                        #instruccion_create = "CREATE TABLE [DEPERTEC].[dbo].[" + str(tabla_ct_nodos) + "] (ID_Caso INT, ID_NODO_LBT_ID VARCHAR(45), Fecha DATE, Hora TIME(7), P_R_KW FLOAT, Q_R_KVAR FLOAT, P_S_KW FLOAT, Q_S_KVAR FLOAT, P_T_KW FLOAT, Q_T_KVAR FLOAT);"
                        # instruccion_create = "CREATE TABLE [DEPERTEC].[dbo].[" + str(tabla_ct_nodos) + "] (ID_Caso INT, ID_NODO_LBT_ID VARCHAR(45), Fecha DATE, Hora TIME(7), P_R_KW FLOAT, P_S_KW FLOAT, P_T_KW FLOAT);"
                        #cursor.execute(instruccion_create)
                    
                    del df_table_exist
                    
                    #Ahora para la tabla de trazas
                    SQL_Query = pd.read_sql_query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '" + str(tabla_ct_trazas) + "'", conn)
                    df_table_exist = pd.DataFrame(SQL_Query, columns=['TABLE_CATALOG','TABLE_SCHEMA','TABLE_NAME','TABLE_TYPE'])
            
                    if len(df_table_exist) > 0:
                        for nodo1, nodo2, keys, data in G.edges(data = True, default = 0, keys=True):         
                            #Se guarda la información de las trazas, sin contar los enlaces con los CUPS. Los CUPS darían error porque solo tienen P y Q de una fase (si son monofásicos)
                            if (G.nodes[nodo1]['Tipo_Nodo'] != 'CUPS') and (G.nodes[nodo2]['Tipo_Nodo'] != 'CUPS') and (G.nodes[nodo1]['Tipo_Nodo'] != 'CUPS_TR') and (G.nodes[nodo2]['Tipo_Nodo'] != 'CUPS_TR'):
                                try:
                                    # instruccion_insert = "INSERT INTO " + tabla_ct_trazas + " (ID_Caso, ID_NODO_LBT_ID_INI, ID_NODO_LBT_ID_FIN, ID_TRAZA, Fecha, Hora, P_R_LINEA_KW, Q_R_LINEA_KVAR, P_S_LINEA_KW, Q_S_LINEA_KVAR, P_T_LINEA_KW, Q_T_LINEA_KVAR) VALUES (" + str(id_caso) + ", '" + str(nodo1) + "', '" + str(nodo2) + "', " + str(keys) + ", '" + str(fecha) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(data['P_R_Linea'])) + ", " + str(float(data['Q_R_Linea'])) + ", " + str(float(data['P_S_Linea'])) + ", " + str(float(data['Q_S_Linea'])) + ", " + str(float(data['P_T_Linea'])) + ", " + str(float(data['Q_T_Linea'])) + ");"
                                    instruccion_insert = "INSERT INTO " + tabla_ct_trazas + " (ID_Caso, ID_NODO_LBT_ID_INI, ID_NODO_LBT_ID_FIN, ID_TRAZA, Fecha, Hora, P_R_LINEA_KW, P_S_LINEA_KW, P_T_LINEA_KW) VALUES (" + str(id_caso) + ", '" + str(nodo1) + "', '" + str(nodo2) + "', " + str(keys) + ", '" + str(fecha_sql) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(data['P_R_Linea'])) + ", " + str(float(data['P_S_Linea'])) + ", " + str(float(data['P_T_Linea'])) + ");"
                #                   print(instruccion_insert)
                                    cursor.execute(instruccion_insert)
                                    conn.commit()
                                    logger.debug(str(colum_hora) + ' guardado correctamente en la tabla de trazas de la BBDD.')
                                except:
                                    logger.error('Error al guardar en la BBDD. ' + instruccion_insert)
                                    continue
                    else:
                        # logger.error('No existe ninguna tabla con nombre ' + tabla_ct_trazas + ' en la BBDD. Ejecutar en el SQL el comando: ' + "CREATE TABLE [DEPERTEC].[dbo].[" + tabla_ct_trazas + "] (ID_Caso INT, ID_NODO_LBT_ID_INI VARCHAR(45), ID_NODO_LBT_ID_FIN VARCHAR(45), ID_TRAZA INT, Fecha DATE, Hora TIME(7), P_R_LINEA_KW FLOAT, Q_R_LINEA_KVAR FLOAT, P_S_LINEA_KW FLOAT, Q_S_LINEA_KVAR FLOAT, P_T_LINEA_KW FLOAT, Q_T_LINEA_KVAR FLOAT);")
                        logger.error('No existe ninguna tabla con nombre ' + str(tabla_ct_trazas) + ' en la BBDD. Ejecutar en el SQL el comando: ' + "CREATE TABLE [DEPERTEC].[dbo].[" + tabla_ct_trazas + "] (ID_Caso INT, ID_NODO_LBT_ID_INI VARCHAR(45), ID_NODO_LBT_ID_FIN VARCHAR(45), ID_TRAZA INT, Fecha DATE, Hora TIME(7), P_R_LINEA_KW FLOAT, Q_R_LINEA_KVAR FLOAT, P_S_LINEA_KW FLOAT, Q_S_LINEA_KVAR FLOAT, P_T_LINEA_KW FLOAT, Q_T_LINEA_KVAR FLOAT);")
                        #instruccion_create = "CREATE TABLE [DEPERTEC].[dbo].[" + str(tabla_ct_trazas) + "] (ID_Caso INT, ID_NODO_LBT_ID_INI VARCHAR(45), ID_NODO_LBT_ID_FIN VARCHAR(45), Fecha DATE, Hora TIME(7), P_R_LINEA_KW FLOAT, Q_R_LINEA_KVAR FLOAT, P_S_LINEA_KW FLOAT, Q_S_LINEA_KVAR FLOAT, P_T_LINEA_KW FLOAT, Q_T_LINEA_KVAR FLOAT);"
                        instruccion_create = "CREATE TABLE [DEPERTEC].[dbo].[" + str(tabla_ct_trazas) + "] (ID_Caso INT, ID_NODO_LBT_ID_INI VARCHAR(45), ID_NODO_LBT_ID_FIN VARCHAR(45), Fecha DATE, Hora TIME(7), P_R_LINEA_KW FLOAT, P_S_LINEA_KW FLOAT, P_T_LINEA_KW FLOAT);"
                        #cursor.execute(instruccion_create)
                    del df_table_exist
                        
                #Se cierra la conexión SQL
                if (self.save_ddbb == 0) or (self.save_ddbb == 1) or (self.save_ddbb == 2):
                    cursor.close()
                    del cursor
        
                # if (self.save_ddbb == 0):
                #     logger.debug(str(colum_hora) + ' guardado correctamente en la BBDD en todas las tablas.')
                # elif (self.save_ddbb == 1):
                #     logger.debug(str(colum_hora) + ' guardado correctamente en la BBDD, pero únicamente en la tabla de agregados CT: ' + self.tabla_cts_general)
                # elif (self.save_ddbb == 2):
                #     logger.debug(str(colum_hora) + ' guardado correctamente en la BBDD, pero únicamente en las tablas de cada CT con todos los datos del grafo: ' + tabla_ct_nodos + ', ' + tabla_ct_trazas + '. NO EN LA TABLA GENERAL DE AGREGADO CT: ' + self.tabla_cts_general)
                # if (self.save_ddbb >= 3) or (self.save_ddbb < 0):
                #     logger.warning('Ningún dato guardado en la BBDD. Cambiar variable "save_ddbb" para guardar.')

        fecha_datetime = fecha_datetime + datetime.timedelta(days=1)
    
    logger.info('Fin de la ejecución: ' + str(time.strftime("%d/%m/%y")) + ' a las ' + str(time.strftime("%H:%M:%S")))
    logger.info('###################################################################')
    # self.update_graph_data_error(graph_data_error)
    return


    





















