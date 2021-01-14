# -*- coding: utf-8 -*-
"""
    File name: graphanalysis.py
    Author: Mario Mañana, David Carriles, GTEA
    Date created: 6 Aug 2020
    Date last modified: 14 Jan 2021
    Python Version: 3.6
    
    Library for creating an Electric Power System Graph and solve losses.
"""


##############################################################################
## GRAPH DEFINITION AND LOSSES ANALYSIS.
##############################################################################
from IPython.display import Image
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
import math
import pyodbc 
import os
import time
import datetime
import logging

import cable # UC Library for Line Loss Analysis and Calculation of Electric Power Systems. Add in the same folder


class Solve_Graph:
    """
    
    Library for creating an Electric Power System Graph and solve losses. 
    Possibility to save results in an SQL DataBase.
    Required cable.py function in the same folder, as well as the code that calls this function.
    
    
    Parameters:
    ----------
    fecha_datetime : # Year, Month, Day to calculate losses [datetime.datetime]
    Nombre_CT : CT name [Ej.: 'XUSTAS']
    ruta_raiz : Project folder [F:\\GTEA\\DEPERTEC\\Grafo\\]
    archivo_topologia : CT nodes file [.csv]
    archivo_traza : CT edges file [.csv]
    archivo_traza_mod : Return file. CT edges modified [.csv]
    archivo_ct_cups : CUPS-CT relation file [.csv]
    ruta_cch : Load curves folder
    archivo_config : SQL configuration file [.txt]
    ruta_log : Logging debug file [.log]
    
    
    
    Other parameters (defined by default):
    ----------
    V_Linea : 400.0 # Line voltage [V]
    X_cable : 0.0 # Line reactance [Ohms/km]
    temp_cables : 20.0 # Cable temperature [ºC]
    save_plt_graph : 0  # Save graph figures configuration parameter [0: Save images. 1: Do not save images]
    guardado_bbdd : 3 # SQL save results method configuration [0: Save all results. 1: Save only general results in 'tabla_cts_general'. 2: Save results only in CT tables. 3: Do not save results]
    tabla_cts_general : OUTPUT_PERDIDAS_AGREGADOS_CT # SQL general table name.
    log_mode : logging.DEBUG # Change logging mode in the ruta_log file. Change DEBUG, INFO, ERROR, WARNING, CRITICAL.
    
    """
    # fecha_datetime # Year, Month, Day to calculate losses [datetime.datetime]
    # Nombre_CT  # CT name [Ej.: 'XUSTAS']
    # ruta_raiz # Project folder
    # archivo_topologia  # CT nodes file [.csv]
    # archivo_traza  # CT edges file [.csv]
    # archivo_traza_mod # Return file. CT edges modified [.csv]
    # archivo_ct_cups   # CUPS-CT relation file [.csv]
    # ruta_cch  # Load curves folder
    # archivo_config  # SQL configuration file [.txt]
    # ruta_log   # Logging debug file [.log]
    
    ### Default parameters
    # V_Linea = 400.0 # Line voltage [V]
    # X_cable = 0.0 # Line reactance [Ohms/km]
    # temp_cables = 20.0 # Cable temperature [ºC]
    # save_plt_graph = 0  # Save graph figures configuration parameter [0: Save images. 1: Do not save images]
    # guardado_bbdd = 3 # SQL save results method configuration [0: Save all results. 1: Save only general results in 'tabla_cts_general'. 2: Save results only in CT tables. 3: Do not save results]
    # tabla_cts_general = 'OUTPUT_PERDIDAS_AGREGADOS_CT' # SQL general table name
    # log_mode = 'logging.INFO' # Change logging mode in the ruta_log file. Change DEBUG, INFO, ERROR, WARNING, CRITICAL.

    
    version = r'Solve Electric Power System Graph Losses Library. v4'
    fecha_datetime: datetime.datetime
    Nombre_CT: str
    ruta_raiz: str
    archivo_topologia: str
    archivo_traza: str
    archivo_traza_mod: str
    archivo_ct_cups: str
    ruta_cch: str
    archivo_config: str
    ruta_log: str
    V_Linea: float
    X_cable: float
    temp_cables: float
    save_plt_graph: int
    guardado_bbdd: int
    tabla_cts_general: str
    log_mode: str
    
    
    def __init__( self, fecha_datetime, Nombre_CT, ruta_raiz, archivo_topologia, archivo_traza, archivo_traza_mod, archivo_ct_cups, ruta_cch, archivo_config, ruta_log, V_Linea=400.0, X_cable=0, temp_cables=20, save_plt_graph=0, guardado_bbdd=3, tabla_cts_general='OUTPUT_PERDIDAS_AGREGADOS_CT', log_mode = 'logging.INFO'):
        self.fecha_datetime = fecha_datetime
        self.Nombre_CT = Nombre_CT
        self.ruta_raiz = ruta_raiz
        self.archivo_topologia = archivo_topologia
        self.archivo_traza = archivo_traza
        self.archivo_traza_mod = archivo_traza_mod
        self.archivo_ct_cups = archivo_ct_cups
        self.ruta_cch = ruta_cch
        self.archivo_config = archivo_config
        self.ruta_log = ruta_log
        self.V_Linea = V_Linea
        self.X_cable = X_cable
        self.temp_cables = temp_cables
        self.save_plt_graph = save_plt_graph
        self.guardado_bbdd = guardado_bbdd
        self.tabla_cts_general = tabla_cts_general
        self.log_mode = log_mode
        
        print(fecha_datetime)
        print(Nombre_CT)
        print(V_Linea)
        print(X_cable)
        print(temp_cables)
        print(archivo_topologia)
        print(archivo_traza)
        print(archivo_traza_mod)
        print(archivo_ct_cups)
        print(ruta_cch)
        print(archivo_config)
        print(ruta_log)
        print(save_plt_graph)
        print(tabla_cts_general)
        print(guardado_bbdd)
        print(log_mode)
        
        main(self)
        
        
       
            
        



##############################################################################
## FUNCTIONS DEFINITION
##############################################################################
    def genera_grafo(self, G, df_nodos_ct, df_traza_ct, LBT_ID_list, id_ct):
        """
        
        Función para generar el grafo con la topología y las trazas dadas.
        Importante: Se replican los nodos y enlaces para generar tantos subgrafos como LBTs haya definidas. Se aplicará el cálculo de pérdidas considerando que cada línea parte con un cableado independiente desde el CT. Para enlazar todo esto con el CT se crean tantos CTs virtuales como LBTs haya.
        
        Parámetros
        ----------
        df_nodos_ct : DataFrame con los nodos del CT indicado (archivo Topología). Se obtiene del archivo 'archivo_nodos'.
        df_traza_ct : DataFrame con las trazas del CT indicado (archivo Trazas). Se obtiene del archivo 'archivo_trazas'.
        LBT_ID_list : Listado de líneas que parten del CT. Se calcula a partir del archivo de Trazas.
        id_ct : Identificador numérico del CT. Se obtiene del archivo de Trazas.
            
        
        Retorno
        -------
        G : Grafo definido. 
            Atributos de los nodos: ID_NODO_LBT_ID, P_R, Q_R, P_S, Q_S, P_T, Q_T, TIPO_NODO, POS(NUDO_X, NUDO_Y).
            Atributos de los enlaces: NODO_ORIGEN_LBT_ID, NODO_DESTINO_LBT_ID, Long, P_R_Linea, Q_R_Linea, P_S_Linea, Q_S_Linea, P_T_Linea, Q_T_Linea, CABLE.
        """
        logger = logging.getLogger('genera_grafo')
        #Primero se genera un grafo que contiene todos los subgrafos por separado en función de los LBT_ID que haya.
        #Después se unen todos los subgrafos en un CT 'ficticio'.
        for index, row in df_nodos_ct.iterrows():
    #        print(index, row['ID_NODO'])
            G.add_node(row['ID_NODO_LBT_ID'], P_R = 0, Q_R = 0, P_S = 0, Q_S = 0, P_T = 0, Q_T = 0, Tipo_Nodo = row['TIPO_NODO'], pos = (float(str(row['NUDO_X']).replace(',','.')), float(str(row['NUDO_Y']).replace(',','.'))))
        
        for index, row in df_traza_ct.iterrows():
        #    print(index, row['ID_VANO_BT'], row['NODO_ORIGEN'], row['NODO_DESTINO'])
        #    print(index, row['ID_VANO'], row['NODO_ORIGEN'], row['NODO_DESTINO'])
            G.add_edge(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])
            #Se añaden atributos
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['Long'] = row['Longitud']
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['P_R_Linea'] = 0
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['Q_R_Linea'] = 0
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['P_S_Linea'] = 0
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['Q_S_Linea'] = 0
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['P_T_Linea'] = 0
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['Q_T_Linea'] = 0
            G.edges[(row['NODO_ORIGEN_LBT_ID'], row['NODO_DESTINO_LBT_ID'])]['CABLE'] = row['CABLE']
        
        #Se añade el CT y se crean los enlaces virtuales entre el ID_CT y los correspondientes ID_CT_LBT_ID  
        for i in range(0,len(LBT_ID_list)):
    #        print(str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))
            G.add_edges_from( [(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))])
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['Long'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['P_R_Linea'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['Q_R_Linea'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['P_S_Linea'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['Q_S_Linea'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['P_T_Linea'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['Q_T_Linea'] = 0
            G.edges[(id_ct, str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i]))]['CABLE'] = df_traza_ct['CABLE'][0]
            #Se añaden también los atributos del nodo que se acaba de crear con idct_lbt
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['P_R'] = 0
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['Q_R'] = 0
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['P_S'] = 0
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['Q_S'] = 0
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['P_T'] = 0
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['Q_T'] = 0
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['Tipo_Nodo'] = 'CT_Virtual'
                    
            id_ct_coord_x = df_nodos_ct.CT_X.drop_duplicates(keep = 'first').reset_index(drop=True)[0]
            id_ct_coord_y = df_nodos_ct.CT_Y.drop_duplicates(keep = 'first').reset_index(drop=True)[0]
            G.nodes[str(id_ct) + '_' + str(LBT_ID_list['LBT_ID'][i])]['pos'] = (float(str(id_ct_coord_x).replace(',','.')), float(str(id_ct_coord_y).replace(',','.')))
    #        print(str(LBT_ID_list['LBT_ID'][i]) +', ' + str(LBT_ID_list['LBT_NOMBRE'][i]))
        
        #Se añaden los atributos al nodo id_ct
        G.nodes[id_ct]['P_R'] = 0
        G.nodes[id_ct]['Q_R'] = 0
        G.nodes[id_ct]['P_S'] = 0
        G.nodes[id_ct]['Q_S'] = 0
        G.nodes[id_ct]['P_T'] = 0
        G.nodes[id_ct]['Q_T'] = 0
        G.nodes[id_ct]['Tipo_Nodo'] = 'CT'
        G.nodes[id_ct]['pos'] = (float(str(id_ct_coord_x).replace(',','.')), str(str(id_ct_coord_y).replace(',','.')))
        
        logger.info('Grafo generado con ' + str(len(G.nodes)) +' nodos y ' + str(len(G.edges)) + ' trazas.')
        
        ########
        #COMPROBAR QUE EXISTE UN CAMINO ENTRE EL CT Y cada nodo.
        ########
        for u,data in G.nodes(data=True, default = 0):
            try:
                ruta=list(nx.shortest_path(G,id_ct, u))
    #            print(ruta)
            except:
                logger.error('Error de descripción de archivos detectado. No hay ruta entre ' + str(id_ct) + ' y ' + str(u) + '.\n')
    #            print('No ruta entre ' + str(id_ct) + ' y ' + str(u))
                continue
        return G
    
    
    def add_cups_grafo(self, G, colum_hora, CUPS_unicos, df_ct_cups_ct, df_matr_dist, df_P_fecha, id_ct, LBT_ID_list, nodos_cups_conectados, nodos_cups_descendientes):
        """
        
        Función para añadir los CUPS al Grafo. Con las curvas de carga de un día y hora concretos añade los CUPS y suma en los nodos asociados las potencias correspondientes. 
        
        Parámetros
        ----------
        G : Grafo definido.
        colum_hora : string que define la columna con los valores horarios de la curva de carga.
        CUPS_unicos : Listado de CUPS diferentes que hay en el CT.
        df_ct_cups_ct : DataFrame con los CUPS y su conexionado el en CT. Se obtiene a partir del archivo 'archivo_ct_cups'.
        df_matr_dist : DataFrame con la matriz de distancias que relaciona cada CUPS con el nodo más cercano y la distancia existente.
        df_P_fecha : DataFrame con las curvas de carga de los CUPS del CT para el día indicado. Se obtiene a partir del directorio 'ruta_cch' y de los archivos ahí contenidos.
        id_ct : Identificador numérico del CT. Se obtiene del archivo de Trazas.
        LBT_ID_list : Listado de líneas que parten del CT. Se calcula a partir del archivo de Trazas.
        nodos_cups_conectados : Listado de nodos del grafo que tienen CUPS conectados y de los que no dependen enlaces inferiores.
        nodos_cups_descendientes : Listado de nodos del grafo que tienen CUPS conectados pero tienen enlaces inferiores.
        
            
        
        Retorno
        -------
        G : Grafo con los CUPS e información de potencias añadida.
        nodos_cups_conectados : Listado actualizado.
        nodos_cups_descendientes : Listado actualizado.
        cups_agregado_CT : Identificador del CUPS que mide a la salida del CT. Hay uno por cada nivel de tensión. Generalmente solo habrá uno, con el agregado de todas las LBT.
        """
        logger = logging.getLogger('add_cups_grafo')
        cups_agregado_CT = [] #Para los cups del agregado a la salida del CT. Debería ser uno para cada nivel de tensión. En BT debería ser solo un CUP pero se comprueba.    
        #Se reinician a 0 todos los valores de potencias en los nodos y en las trazas. Para las iteraciones por horas
        for nodo1, nodo2, data in G.edges(data = True, default = 0):
            if (G.nodes[nodo1]['Tipo_Nodo'] != 'CUP') and (G.nodes[nodo2]['Tipo_Nodo'] != 'CUP'):
                G.edges[nodo1, nodo2]['P_R_Linea'] = 0
                G.edges[nodo1, nodo2]['P_S_Linea'] = 0
                G.edges[nodo1, nodo2]['P_T_Linea'] = 0
                G.edges[nodo1, nodo2]['Q_R_Linea'] = 0
                G.edges[nodo1, nodo2]['Q_S_Linea'] = 0
                G.edges[nodo1, nodo2]['Q_T_Linea'] = 0
                
        for nodo,data in G.nodes(data=True, default = 0):
            if (G.nodes[nodo]['Tipo_Nodo'] != 'CUP'):
                G.nodes[nodo]['P_R'] = 0
                G.nodes[nodo]['P_S'] = 0
                G.nodes[nodo]['P_T'] = 0
                G.nodes[nodo]['Q_R'] = 0
                G.nodes[nodo]['Q_S'] = 0
                G.nodes[nodo]['Q_T'] = 0    
        
        for row in CUPS_unicos:
            if (len(df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]) > 0):
                #Se obtienen datos del tipo de suministro. Comprobar que no se trata de los contadores del CT
                #Los del CT tienen un valor numérico en la columna CTE_GISS, el resto vacío
                if (df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['CTE_GISS'].reset_index(drop=True)[0]) > 0:
    #                print('CUPS para el agregado a la salida del CT: ' + row)
                    cups_agregado_CT.append(row)
                else:
                    cup_lbt_nombre = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['LBT_NOMBRE'].reset_index(drop=True)[0] #Número de la línea
                    cup_lbt_id = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['LBT_ID'].reset_index(drop=True)[0] #Número de la línea
                    cup_amm_fase = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['AMM_FASE'].reset_index(drop=True)[0] #Fase de conexión. R, S, T
                    cup_tipo_conexion = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['TIPO_CONEXION'].reset_index(drop=True)[0] #Monofásico / trifásico
                    cup_pot_max = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['POT_MAX'].reset_index(drop=True)[0]  # kW
                    cup_tipo_actividad = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['TIPO_ACTIVIDAD'].reset_index(drop=True)[0] #Residencial, Industria, Servicios
                    
                    arqueta = df_matr_dist.loc[df_matr_dist['CUPS'] == row]['ID_Nodo_Cercano'].reset_index(drop=True)[0]
                    arqueta_cup_lbt_id = str(arqueta) + '_' + str(cup_lbt_id)
                  
                    ########
                    #COMPROBAR SI arqueta_cup_lbt_id EXISTE EN EL GRAFO
                    #Si no existe, se asocia el CUP al NODO_LBT_ID que exista. (Se asume el error pero se garantiza que el CUP queda conectado al grafo)
                    ########
                    if arqueta_cup_lbt_id not in G.nodes:
    #                    print(arqueta_cup_lbt_id)
                        for lbt_row in LBT_ID_list.itertuples():
                            arqueta_temp = str(arqueta) + '_' + str(lbt_row.LBT_ID)
                            print(arqueta_temp)
                            if arqueta_temp in G.nodes:
                                logger.warning('Nodo con ID_NODO_LBT_ID ' + arqueta_cup_lbt_id + ' no encontrada. Se asocia el CUP_LBT_ID ' +  str(row) + '_' + str(cup_lbt_id) + ' al ID_NODO_LBT_ID ' + arqueta_temp)
                                print('Nodo con ID_NODO_LBT_ID ' + arqueta_cup_lbt_id + ' no encontrada. Se asocia el CUP_LBT_ID ' +  str(row) + '_' + str(cup_lbt_id) + ' al ID_NODO_LBT_ID ' + arqueta_temp)
                                arqueta_cup_lbt_id = arqueta_temp
                                break
                    
                    if df_P_fecha.loc[df_P_fecha['CUPS'] == row][colum_hora].reset_index(drop=True)[0] > 0:
                        potencia_cup = df_P_fecha.loc[df_P_fecha['CUPS'] == row][colum_hora].reset_index(drop=True)[0]
                    else:
                        potencia_cup = 0
                    longitud = df_matr_dist.loc[df_matr_dist['CUPS'] == row]['Distancia'].reset_index(drop=True)[0]
                    
                    #Se comprueba si la arqueta tiene más de 1 enlace previo (no sería arqueta de fin de línea sino que tendría otros sucesores)
                    if G.degree(arqueta_cup_lbt_id)==1 and (arqueta_cup_lbt_id not in nodos_cups_conectados):
                        nodos_cups_conectados.append(arqueta_cup_lbt_id)
                    elif G.degree(arqueta_cup_lbt_id)>1 and(arqueta_cup_lbt_id not in nodos_cups_conectados) and (arqueta_cup_lbt_id not in nodos_cups_descendientes):
                        nodos_cups_descendientes.append(arqueta_cup_lbt_id)
                        
                    #Una vez comprobados los enlaces del nodo se añade el CUP
                    if str(row) + '_' + str(cup_lbt_id) not in G.nodes:
                        G.add_edges_from( [(arqueta_cup_lbt_id, str(row) + '_' + str(cup_lbt_id))])
                    G.nodes[str(row) + '_' + str(cup_lbt_id)]['P_' + cup_amm_fase] = potencia_cup
                    G.nodes[str(row) + '_' + str(cup_lbt_id)]['Q_' + cup_amm_fase] = 0
                    G.nodes[str(row) + '_' + str(cup_lbt_id)]['TIPO_CONEXION'] = cup_tipo_conexion
                    G.nodes[str(row) + '_' + str(cup_lbt_id)]['AMM_FASE'] = cup_amm_fase
                    G.nodes[str(row) + '_' + str(cup_lbt_id)]['Tipo_Nodo'] = 'CUP'
                            
                    id_ct_coord_x = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['CUPS_X'].reset_index(drop=True)[0]
                    id_ct_coord_y = df_ct_cups_ct.loc[df_ct_cups_ct['CUPS'] == row]['CUPS_Y'].reset_index(drop=True)[0]
                    G.nodes[str(row) + '_' + str(cup_lbt_id)]['pos'] = (float(str(id_ct_coord_x).replace(',','.')), str(str(id_ct_coord_y).replace(',','.')))
                    
                    #Se agrega la potencia en el nodo. Aquí se sumará lo de todos los CUPS conectados a ese nodo.
                    if cup_tipo_conexion == 'MONOFASICO':
                        G.nodes[arqueta_cup_lbt_id]['P_' + cup_amm_fase] = G.nodes[arqueta_cup_lbt_id]['P_' + cup_amm_fase] + potencia_cup
                        G.nodes[arqueta_cup_lbt_id]['Q_' + cup_amm_fase] = G.nodes[arqueta_cup_lbt_id]['Q_' + cup_amm_fase] + G.nodes[ str(row) + '_' + str(cup_lbt_id)]['Q_' + cup_amm_fase]
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['Long'] = float(longitud)
                        #Pérdidas de la línea que une la arqueta con el CUP. Puede ser trifásica o monofásica. NO tenemos el tipo de cable.
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['P_' + cup_amm_fase + '_Linea'] = 0
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['Q_' + cup_amm_fase + '_Linea'] = 0
                    elif cup_tipo_conexion == 'TRIFASICO':
                        G.nodes[arqueta_cup_lbt_id]['P_R'] = G.nodes[arqueta_cup_lbt_id]['P_R'] + potencia_cup/3
                        G.nodes[arqueta_cup_lbt_id]['Q_R'] = G.nodes[arqueta_cup_lbt_id]['Q_R'] + G.nodes[ str(row) + '_' + str(cup_lbt_id)]['Q_' + cup_amm_fase]/3
                        G.nodes[arqueta_cup_lbt_id]['P_S'] = G.nodes[arqueta_cup_lbt_id]['P_S'] + potencia_cup/3
                        G.nodes[arqueta_cup_lbt_id]['Q_S'] = G.nodes[arqueta_cup_lbt_id]['Q_S'] + G.nodes[ str(row) + '_' + str(cup_lbt_id)]['Q_' + cup_amm_fase]/3
                        G.nodes[arqueta_cup_lbt_id]['P_T'] = G.nodes[arqueta_cup_lbt_id]['P_T'] + potencia_cup/3
                        G.nodes[arqueta_cup_lbt_id]['Q_T'] = G.nodes[arqueta_cup_lbt_id]['Q_T'] + G.nodes[ str(row) + '_' + str(cup_lbt_id)]['Q_' + cup_amm_fase]/3
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['Long'] = float(longitud)
                        #Pérdidas de la línea que une la arqueta con el CUP. Puede ser trifásica o monofásica. NO tenemos el tipo de cable.
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['P_R_Linea'] = 0
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['Q_R_Linea'] = 0
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['P_S_Linea'] = 0
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['Q_S_Linea'] = 0
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['P_T_Linea'] = 0
                        G.edges[(arqueta_cup_lbt_id,  str(row) + '_' + str(cup_lbt_id))]['Q_T_Linea'] = 0
                    else:
                        logger.error('ERROR CUPS. TIPO DE CONEXIÓN NO IDENTIFICADA (MONOFÁSICA/TRIFÁSICA): ' +  str(row) + '_' + str(cup_lbt_id) + ': ' + cup_tipo_conexion + '. SI SE TRATA DE CUPS DEL AGREGADO CT NO HAY PROBLEMA.')
    #                    print('ERROR CUPS. TIPO DE CONEXIÓN NO IDENTIFICADA (MONOFÁSICA/TRIFÁSICA): ',  str(row) + '_' + str(cup_lbt_id))
    #                    print('SI SE TRATA DE CUPS DEL AGREGADO CT NO HAY PROBLEMA')
            
        #Se eliminan los duplicados en los nodos con CUPs ya conectados    
        nodos_cups_conectados = list(dict.fromkeys(nodos_cups_conectados))
        nodos_cups_descendientes = list(dict.fromkeys(nodos_cups_descendientes))
        
        #Se comprueba el número de CUPS de agregado CT que se han encontrado. Si es más de 1 revisar
        cups_agregado_CT = list(dict.fromkeys(cups_agregado_CT))
        if len(cups_agregado_CT) > 1:
            logger.warning('ENCONTRADO MÁS DE 1 CUP DE AGREGADO AL CT. REVISAR: ' + cups_agregado_CT)
    #        print('ENCONTRADO MÁS DE 1 CUP DE AGREGADO AL CT. REVISAR: ' + cups_agregado_CT)
        
        return G, nodos_cups_conectados, nodos_cups_descendientes, cups_agregado_CT
    
    
    def resuelve_grafo(self, G, end_nodes_cups, id_ct, splitting_nodes_sin_cups, temp_cables):
        """
        
        Función para resolver el grafo y calcular pérdidas a partir de los datos introducidos. Emplea la librería cables.py para obtener la resistencia para el tipo de cable de cada traza.
        
        Parámetros
        ----------
        G : Grafo definido.
        end_nodes_cups : Listado con todos los nodos que pueden ser terminación de línea y/o tener CUPs conectados.
        id_ct : Identificador numérico del CT. Se obtiene del archivo de Trazas.
        splitting_nodes_sin_cups : Listado con todos los nodos que tienen más de un predecesor-sucesor.
            
        
        Retorno
        -------
        G : Grafo actualizado con los datos de potencias agregadas en cada nodo y de pérdidas en cada traza.
        """
        logger = logging.getLogger('resuelve_grafo')
        #Hay que asegurarse de que todos los nodos tienen al atributo 'Enlaces_iter' igual que 'Enlaces_orig'
        for row in splitting_nodes_sin_cups:
            G.nodes[row]['Enlaces_iter'] = G.nodes[row]['Enlaces_orig']
        
        #Se recorren todos los nodos finales (los que no tienen conectado nada y los que tienen conectados los CUPS pero NO tienen otros descendientes)
        for row in end_nodes_cups:
            ruta=list(nx.shortest_path(G,id_ct,row))
            ruta.remove(row)
            
            #Se recorre la ruta en sentido inverso, añadiendo la potencia y calculando las pérdidas de los vanos.
            #Se para cuando se encuentra un nodo con más ramificaciones.
            nodo_old = row
            for row2 in reversed(ruta):
                #Si la longitud de la traza es distinta de 0 se calculan las pérdidas. si es 0 puede ser un enlace del CT virtual que no tiene asignado tipo de cable
    #            if float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.')) != 0:
                tipo_cable = str(G.edges[(row2, nodo_old)]['CABLE'])
                I_R_row = math.sqrt(float(G.nodes[nodo_old]['P_R']*1000)**2 + float(G.nodes[nodo_old]['Q_R']*1000)**2)/(self.V_Linea)
                I_S_row = math.sqrt(float(G.nodes[nodo_old]['P_S']*1000)**2 + float(G.nodes[nodo_old]['Q_S']*1000)**2)/(self.V_Linea)
                I_T_row = math.sqrt(float(G.nodes[nodo_old]['P_T']*1000)**2 + float(G.nodes[nodo_old]['Q_T']*1000)**2)/(self.V_Linea)
                
                #Se llama a la librería de cálculo de resistencia por km
                Cable_R = cable.Conductor()
    #                Cable_R.fload_library( "RZ_0.6/1.0_kV_4x10_CU")
                Found_cable = Cable_R.fload_library( tipo_cable)
                if Found_cable == 0:
                    logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '. Se consideran valores definidos por defecto.')
                Cable_R.fset_t1(temp_cables)
                Cable_R.fset_i(I_R_row)
                R_cable_ohm_km_R = Cable_R.fcompute_r()
                R_cable_ohm_R = R_cable_ohm_km_R * float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.'))/1000
                
                Cable_S = cable.Conductor()
                Found_cable = Cable_S.fload_library( tipo_cable)
                if Found_cable == 0:
                    logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '. Se consideran valores definidos por defecto.')
                Cable_S.fset_t1(temp_cables)
                Cable_S.fset_i(I_S_row)
                R_cable_ohm_km_S = Cable_S.fcompute_r()
                R_cable_ohm_S = R_cable_ohm_km_S * float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.'))/1000
                
                Cable_T = cable.Conductor()
                Found_cable = Cable_T.fload_library( tipo_cable)
                if Found_cable == 0:
                    logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '. Se consideran valores definidos por defecto.')
                Cable_T.fset_t1(temp_cables)
                Cable_T.fset_i(I_T_row)
                R_cable_ohm_km_T = Cable_T.fcompute_r()
                R_cable_ohm_T = R_cable_ohm_km_T * float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.'))/1000
                
                P_R_row = R_cable_ohm_R*I_R_row**2/1000 #Se pasa a kW
                P_S_row = R_cable_ohm_S*I_S_row**2/1000 #Se pasa a kW
                P_T_row = R_cable_ohm_T*I_T_row**2/1000 #Se pasa a kW
                
                Q_R_row = self.X_cable*I_R_row**2/1000
                Q_S_row = self.X_cable*I_S_row**2/1000
                Q_T_row = self.X_cable*I_T_row**2/1000
                
                G.edges[(row2, nodo_old)]['P_R_Linea'] = G.edges[(row2, nodo_old)]['P_R_Linea'] + P_R_row
                G.nodes[row2]['P_R'] = G.nodes[row2]['P_R'] + G.nodes[nodo_old]['P_R'] + P_R_row
                G.edges[(row2, nodo_old)]['Q_R_Linea'] = G.edges[(row2, nodo_old)]['Q_R_Linea'] + Q_R_row
                G.nodes[row2]['Q_R'] = G.nodes[row2]['Q_R'] + G.nodes[nodo_old]['Q_R'] + Q_R_row
                
                G.edges[(row2, nodo_old)]['P_S_Linea'] = G.edges[(row2, nodo_old)]['P_S_Linea'] + P_S_row
                G.nodes[row2]['P_S'] = G.nodes[row2]['P_S'] + G.nodes[nodo_old]['P_S'] + P_S_row
                G.edges[(row2, nodo_old)]['Q_S_Linea'] = G.edges[(row2, nodo_old)]['Q_S_Linea'] + Q_S_row
                G.nodes[row2]['Q_S'] = G.nodes[row2]['Q_S'] + G.nodes[nodo_old]['Q_S'] + Q_S_row
                
                G.edges[(row2, nodo_old)]['P_T_Linea'] = G.edges[(row2, nodo_old)]['P_T_Linea'] + P_T_row
                G.nodes[row2]['P_T'] = G.nodes[row2]['P_T'] + G.nodes[nodo_old]['P_T'] + P_T_row
                G.edges[(row2, nodo_old)]['Q_T_Linea'] = G.edges[(row2, nodo_old)]['Q_T_Linea'] + Q_T_row
                G.nodes[row2]['Q_T'] = G.nodes[row2]['Q_T'] + G.nodes[nodo_old]['Q_T'] + Q_T_row
                
                if row2 in splitting_nodes_sin_cups:
                    G.nodes[row2]['Enlaces_iter'] -=1
                    break 
                nodo_old = row2
                
        #Ahora se recorre de forma iterativa los splitting nodes que ya tienen 'Enlaces_iter' == 1 hasta que se llega a otro splitting node con más ramas.
        #Así hasta acabar con todo el grafo.
        nodos_utilizados = []
        while len(nodos_utilizados) < len(splitting_nodes_sin_cups):        
            for row in splitting_nodes_sin_cups:
                if G.nodes[row]['Enlaces_iter'] == 1 and (row not in (nodos_utilizados)):
                    ruta=list(nx.shortest_path(G,id_ct,row))
                    ruta.remove(row)
                    #Se recorre la ruta en sentido inverso, añadiendo la potencia y calculando las pérdidas de los vanos.
                    #Hasta que se encuentra un nodo con más ramificaciones
                    nodo_old = row
                    for row2 in reversed(ruta):
                        #Si la longitud de la traza es distinta de 0 se calculan las pérdidas. si es 0 puede ser un enlace del CT virtual que no tiene asignado tipo de cable
    #                    if float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.')) != 0:
                        tipo_cable = str(G.edges[(row2, nodo_old)]['CABLE'])
                        I_R_row = math.sqrt(float(G.nodes[nodo_old]['P_R']*1000)**2 + float(G.nodes[nodo_old]['Q_R']*1000)**2)/(math.sqrt(3)*self.V_Linea)
                        I_S_row = math.sqrt(float(G.nodes[nodo_old]['P_S']*1000)**2 + float(G.nodes[nodo_old]['Q_S']*1000)**2)/(math.sqrt(3)*self.V_Linea)
                        I_T_row = math.sqrt(float(G.nodes[nodo_old]['P_T']*1000)**2 + float(G.nodes[nodo_old]['Q_T']*1000)**2)/(math.sqrt(3)*self.V_Linea)                
                        
                        #Se llama a la librería de cálculo de resistencia por km
                        Cable_R = cable.Conductor()
                        Found_cable = Cable_R.fload_library( tipo_cable)
                        if Found_cable == 0:
                            logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '. Se consideran valores definidos por defecto.')
                        Cable_R.fset_t1(temp_cables)
                        Cable_R.fset_i(I_R_row)
                        R_cable_ohm_km_R = Cable_R.fcompute_r()
                        R_cable_ohm_R = R_cable_ohm_km_R * float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.'))/1000
                        
                        Cable_S = cable.Conductor()
                        Found_cable = Cable_S.fload_library( tipo_cable)
                        if Found_cable == 0:
                            logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '. Se consideran valores definidos por defecto.')
                        Cable_S.fset_t1(temp_cables)
                        Cable_S.fset_i(I_S_row)
                        R_cable_ohm_km_S = Cable_S.fcompute_r()
                        R_cable_ohm_S = R_cable_ohm_km_S * float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.'))/1000
                        
                        Cable_T = cable.Conductor()
                        Found_cable = Cable_T.fload_library( tipo_cable)
                        if Found_cable == 0:
                            logger.warning('Error al buscar el tipo de cable ' + tipo_cable + ' en la librería .xml. Enlace ' + str(row2) + '-' + str(nodo_old) + '. Se consideran valores definidos por defecto.')
                        Cable_T.fset_t1(temp_cables)
                        Cable_T.fset_i(I_T_row)
                        R_cable_ohm_km_T = Cable_T.fcompute_r()
                        R_cable_ohm_T = R_cable_ohm_km_T * float(str(G.edges[(row2, nodo_old)]['Long']).replace(',','.'))/1000
                        
                        P_R_row = R_cable_ohm_R*I_R_row**2/1000 #Se pasa a kW
                        P_S_row = R_cable_ohm_S*I_S_row**2/1000 #Se pasa a kW
                        P_T_row = R_cable_ohm_T*I_T_row**2/1000 #Se pasa a kW
                        
                        Q_R_row = self.X_cable*I_R_row**2/1000
                        Q_S_row = self.X_cable*I_S_row**2/1000
                        Q_T_row = self.X_cable*I_T_row**2/1000                    
                        
                        G.edges[(row2, nodo_old)]['P_R_Linea'] = G.edges[(row2, nodo_old)]['P_R_Linea'] + P_R_row
                        G.nodes[row2]['P_R'] = G.nodes[row2]['P_R'] + G.nodes[nodo_old]['P_R'] + P_R_row
                        G.edges[(row2, nodo_old)]['Q_R_Linea'] = G.edges[(row2, nodo_old)]['Q_R_Linea'] + Q_R_row
                        G.nodes[row2]['Q_R'] = G.nodes[row2]['Q_R'] + G.nodes[nodo_old]['Q_R'] + Q_R_row
                        
                        G.edges[(row2, nodo_old)]['P_S_Linea'] = G.edges[(row2, nodo_old)]['P_S_Linea'] + P_S_row
                        G.nodes[row2]['P_S'] = G.nodes[row2]['P_S'] + G.nodes[nodo_old]['P_S'] + P_S_row
                        G.edges[(row2, nodo_old)]['Q_S_Linea'] = G.edges[(row2, nodo_old)]['Q_S_Linea'] + Q_S_row
                        G.nodes[row2]['Q_S'] = G.nodes[row2]['Q_S'] + G.nodes[nodo_old]['Q_S'] + Q_S_row
                        
                        G.edges[(row2, nodo_old)]['P_T_Linea'] = G.edges[(row2, nodo_old)]['P_T_Linea'] + P_T_row
                        G.nodes[row2]['P_T'] = G.nodes[row2]['P_T'] + G.nodes[nodo_old]['P_T'] + P_T_row
                        G.edges[(row2, nodo_old)]['Q_T_Linea'] = G.edges[(row2, nodo_old)]['Q_T_Linea'] + Q_T_row
                        G.nodes[row2]['Q_T'] = G.nodes[row2]['Q_T'] + G.nodes[nodo_old]['Q_T'] + Q_T_row
                        
                        if row2 in splitting_nodes_sin_cups:
                            if G.nodes[row2]['Enlaces_iter'] > 2:
                                G.nodes[row2]['Enlaces_iter'] -=1
                                break 
                            nodos_utilizados.append(row2)
                        nodo_old = row2
                    nodos_utilizados.append(row)
                    nodos_utilizados = list(dict.fromkeys(nodos_utilizados))
        return G
    
   
    
    



##############################################################################
## Código main
##############################################################################
def main(self):
    #Adaptación de la fecha para crear un ID para el SQL
    fecha = int(str(self.fecha_datetime.strftime("%Y")) + str(self.fecha_datetime.strftime("%m")) + str(self.fecha_datetime.strftime("%d")))
    #Diccionario de horas con los nombres de todas las columnas de las curgas de carga
    diccionario_horas = {'VALOR_H01': '01', 'VALOR_H02': '02', 'VALOR_H03': '03', 'VALOR_H04': '04', 'VALOR_H05': '05', 'VALOR_H06': '06', 'VALOR_H07': '07', 'VALOR_H08': '08', 'VALOR_H09': '09', 'VALOR_H10': '10', 'VALOR_H11': '11', 'VALOR_H12': '12', 'VALOR_H13': '13', 'VALOR_H14': '14', 'VALOR_H15': '15', 'VALOR_H16': '16', 'VALOR_H17': '17', 'VALOR_H18': '18', 'VALOR_H19': '19', 'VALOR_H20': '20', 'VALOR_H21': '21', 'VALOR_H22': '22', 'VALOR_H23': '23', 'VALOR_H24': '00'}#, 'VALOR_H25': '25'}
    
    plt_graph_file = self.ruta_raiz + 'Graph_' + self.Nombre_CT + '_v1.jpg'
    plt_graph_file_v2 = self.ruta_raiz + 'Graph_' + self.Nombre_CT + '_v2.jpg'
    
    # Se eliminan los handlers anteriores
#    if logging.getLogger('').hasHandlers():
#        logging.getLogger('').handlers.clear()
    
    logging.basicConfig(filename=self.ruta_log, level=eval(self.log_mode), filemode='w')
    logger = logging.getLogger(__name__)
    logger.setLevel(eval(self.log_mode))
    
    logger.info('###################################################################')
    logger.info('Ejecutado el ' + str(time.strftime("%d/%m/%y")) + ' a las ' + str(time.strftime("%H:%M:%S")))
    logger.info('CT seleccionado: ' + self.Nombre_CT + '. Fecha de análisis: ' + str(fecha))
    logger.info('V_Linea=' + str(self.V_Linea) + '. X_cable=' + str(self.X_cable) + '. temp_cables=' + str(self.temp_cables))
    logger.info('###################################################################')
    logger.info('Archivo topología: ' + self.archivo_topologia + '. Archivo trazas: ' + self.archivo_traza + '. Archivo CUPS: ' + self.archivo_ct_cups + '. Archivo trazas modificado: ' + self.archivo_traza_mod + '. Ruta curvas de carga: ' + self.ruta_cch + '. Archivo config. SQL: ' + self.archivo_config + '. Guardado de las imagenes del grafo: ' + str(self.save_plt_graph) + '. Imagen 1: ' + plt_graph_file + ', imagen 2: ' + plt_graph_file_v2 + '. Guardado de resultados en SQL: ' + str(self.guardado_bbdd) + '. Logging mode: ' + self.log_mode)
    
    
    #Creación de DataFrames con las tablas
    df_nodos = pd.read_csv(self.archivo_topologia, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal=',')
    #Necesario cambiar algún tipo de datos para que no lo considere como número e incluya decimales .0
    df_nodos['ID_NODO'] = df_nodos['ID_NODO'].apply(str).replace('.0', '')#.str.strip('.0')
    df_nodos['LBT_NOMBRE'] = df_nodos['LBT_NOMBRE'].apply(str).replace('.0', '')#.str.strip('.0')
    df_nodos['LBT_ID'] = df_nodos['LBT_ID'].apply(str).replace('.0', '')#.str.strip('.0')
    df_nodos_ct = df_nodos[df_nodos['CT_NOMBRE'] == self.Nombre_CT].reset_index(drop=True)
    
    if len(df_nodos_ct) > 0:
        logger.debug('.CSV de nodos leído. Archivo completo: df_nodos = '+  str(len(df_nodos)) + '; filtro por CT indicado: df_nodos_ct = ' + str(len(df_nodos_ct)))
    else:
        logger.debug('.CSV de nodos leído. Archivo completo: df_nodos = '+  str(len(df_nodos)) + '; filtro por CT indicado: df_nodos_ct = ' + str(len(df_nodos_ct)))
        logger.error('Error al filtrar los nodos por el nombre del CT indicado: ' + self.Nombre_CT + '. Revisar que el nombre se corresponde con la columna "CT_NOMBRE" del .CSV o que el archivo indicado es el adecuado.')
    
    df_traza = pd.read_csv(self.archivo_traza, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal = ',')
    df_traza['NODO_ORIGEN'] = df_traza['NODO_ORIGEN'].apply(str).replace('.0', '')#.str.strip('.0')
    df_traza['NODO_DESTINO'] = df_traza['NODO_DESTINO'].apply(str).replace('.0', '')#.str.strip('.0')
    df_traza['LBT_ID'] = df_traza['LBT_ID'].apply(str).replace('.0', '')#.str.strip('.0')
    
    logger.debug('.CSV de trazas leído. Archivo completo: df_traza = '+  str(len(df_traza)) + '. Creando columna de longitudes.')
    
    #Se crea la columna 'Longitud' con la longitud de cada traza y se guarda en un .csv
    df_traza = df_traza.reindex(columns = df_traza.columns.tolist() + ["Longitud"])
    for index, row in df_traza.iterrows():
        try:
            long_calc = math.sqrt((row['X_DESTINO'] - row['X_ORIGEN'])**2 + (row['Y_DESTINO'] - row['Y_ORIGEN'])**2)
            if long_calc >= 0:
                df_traza.loc[index, 'Longitud'] = long_calc
            else:
                df_traza.loc[index, 'Longitud'] = 0
                print('Error al calcular la longitud para la traza: NODO_ORIGEN = ' + str(row.NODO_ORIGEN) + '; NODO_DESTINO = ' + str(row.NODO_DESTINO) + '. Valor obtenido: ' + str(long_calc) + '. Asignado valor 0.')
                logger.error('Error al calcular la longitud para la traza: NODO_ORIGEN = ' + str(row.NODO_ORIGEN) + '; NODO_DESTINO = ' + str(row.NODO_DESTINO) + '. Valor obtenido: ' + str(long_calc) + '. Asignado valor 0.')
        except:
            df_traza.loc[index, 'Longitud'] = 0
            logger.error('Error de código al calcular longitud para la traza: NODO_ORIGEN = ' + str(row.NODO_ORIGEN) + '; NODO_DESTINO = ' + str(row.NODO_DESTINO) + '. Valor obtenido: ' + str(long_calc) + '. Asignado valor 0.')
            continue
    #Se adaptan los nombres de los cables
    df_traza['CABLE'] = df_traza['CABLE'].str.replace(' ', '_').str.replace(',','.')
    df_traza.to_csv(self.archivo_traza_mod, index = False, encoding='Latin9', sep=';', decimal=',')
    #Se filtran los datos del CT seleccionado
    df_traza_ct = df_traza[df_traza['CT_NOMBRE'] == self.Nombre_CT].reset_index(drop=True)
    
    if len(df_traza_ct) > 0:
        logger.debug('Longitudes calculadas y guardadas en el archivo: ' + self.archivo_traza_mod + ' Cambio de formato a la columna "CABLE". Filtro por CT indicado: df_traza_ct = ' + str(len(df_traza_ct)))
    else:
        logger.debug('Longitudes calculadas y guardadas en el archivo: ' + self.archivo_traza_mod + ' Cambio de formato a la columna "CABLE". Filtro por CT indicado: df_traza_ct = ' + str(len(df_traza_ct)))
        logger.error('Error al filtrar las trazas por el nombre del CT indicado: ' + self.Nombre_CT + '. Revisar que el nombre se corresponde con la columna "CT_NOMBRE" del .CSV o que el archivo indicado es el adecuado.')
    
    
    df_ct_cups = pd.read_csv(self.archivo_ct_cups, encoding='Latin9', header=0, sep=';', quotechar='\"', decimal=',')
    df_ct_cups['LBT_NOMBRE'] = df_ct_cups['LBT_NOMBRE'].apply(str).replace('.0', '')#.str.strip('.0')
    df_ct_cups['LBT_ID'] = df_ct_cups['LBT_ID'].apply(str).str.strip('.0')#.astype('str')#str.strip(' ')
    df_ct_cups['AMM_FASE'] = df_ct_cups['AMM_FASE'].str.upper() #Poner en mayúsculas la columna de las fases, para evitar errores.
    df_ct_cups_ct = df_ct_cups[df_ct_cups['CT_NOMBRE'] == self.Nombre_CT].drop_duplicates(keep = 'first').reset_index(drop=True)
    
    if len(df_ct_cups_ct) > 0:
        logger.debug('.CSV con la info de los CUPS leído. Archivo completo: df_ct_cups = '+  str(len(df_ct_cups)) + '; filtro por CT indicado: df_ct_cups_ct = ' + str(len(df_ct_cups_ct)) + '. Calculando matriz de distancias entre cada CUPS y su nodo más cercano.')
    else:
        logger.debug('.CSV con la info de los CUPS leído. Archivo completo: df_ct_cups = '+  str(len(df_ct_cups)) + '; filtro por CT indicado: df_ct_cups_ct = ' + str(len(df_ct_cups_ct)) + '. Calculando matriz de distancias entre cada CUPS y su nodo más cercano.')
        logger.error('Error al filtrar los CUPS por el nombre del CT indicado: ' + self.Nombre_CT + '. Revisar que el nombre se corresponde con la columna "CT_NOMBRE" del .CSV o que el archivo indicado es el adecuado.')
    
    #Matriz de distancias
    #CUIDADO, el separador que devuelve QGIs es la , no el ;
    #Creación de la matriz de distancias. Columnas: CUPS, Arqueta más cercana y distancia.
    df_matr_dist = []
    df_matr_dist = df_ct_cups_ct[['CUPS']] #Selección de todos los CUPS del CT
    df_matr_dist = df_matr_dist.reindex(columns = df_matr_dist.columns.tolist() + ["ID_Nodo_Cercano","Distancia"]) #Añadir las 2 columnas vacías
    
    for index, row in df_ct_cups_ct.iterrows():
        distancia = 999999 #Valor muy grande de distancia, para actualizarlo hasta encontrar el valor más pequeño.
        for index2, row2 in df_nodos_ct.iterrows():
            distancia_prov = math.sqrt((float(row['CUPS_X']) - float(row2['NUDO_X']))**2 + (float(row['CUPS_Y']) - float(row2['NUDO_Y']))**2)
            if distancia_prov < distancia:
                distancia = distancia_prov
                df_matr_dist.loc[index, 'ID_Nodo_Cercano'] = row2['ID_NODO']
                df_matr_dist.loc[index, 'Distancia'] = distancia
    df_matr_dist.to_csv(self.ruta_raiz + r'Matr_Dist_' + self.Nombre_CT +'.csv', index = False, encoding='Latin9', sep=';', decimal=',')
    
    if len(df_matr_dist) > 0:
        logger.debug('Matriz de distancia calculada y guardada en el archivo: ' + self.ruta_raiz + r'Matr_Dist_' + self.Nombre_CT +'.csv' + ' para el CT seleccionado.')
    else:
        logger.error('Error al calcular la matriz de distancias. Revisar datos.')
    
    #Lectura de datos para la conexión con la DB SQL
    try:
        f = open (self.archivo_config,'r')
        ip_server = f.readline().splitlines()[0]
        db_server = f.readline().splitlines()[0]
        usr_server = f.readline().splitlines()[0]
        pwd_server = f.readline().splitlines()[0]
        f.close()
    except:
        logger.error('Error al leer los datos de conexión con la DDBB.')
        
    
    #Se modifica el DF df_nodos_ct para añadir la columna ID_NODO_LBT_ID. Se crea un nodo para cada LBT asociada al mismo
    try:
        temp=[]
        temp = df_nodos_ct['ID_NODO'].apply(str) + '_' + df_nodos_ct['LBT_ID'].apply(str).str.strip('.0')
        df_nodos_ct['ID_NODO_LBT_ID'] = temp
        
        #Se modifica el DF df_traza_ct para añadir las columnas NODO_ORIGEN_ID_VANO_BT y NODO_DESTINO_ID_VANO_BT
        temp=[]
        temp = df_traza_ct['NODO_ORIGEN'].apply(str) + '_' + df_traza_ct['LBT_ID'].apply(str).str.strip('.0')
        df_traza_ct['NODO_ORIGEN_LBT_ID'] = temp
        temp=[]
        temp = df_traza_ct['NODO_DESTINO'].apply(str) + '_' + df_traza_ct['LBT_ID'].apply(str).str.strip('.0')
        df_traza_ct['NODO_DESTINO_LBT_ID'] = temp
    except:
        logger.error('Error al calcular los nuevos IDs de los nodos y trazas asociados con las LBT. Datos columnas "ID_NODO_LBT_ID": ' + str(len(df_nodos_ct['ID_NODO_LBT_ID'])) + ', "NODO_ORIGEN_LBT_ID": ' + str(len(df_traza_ct['NODO_ORIGEN_LBT_ID'])) + ', "NODO_DESTINO_LBT_ID": ' + str(len(df_traza_ct['NODO_DESTINO_LBT_ID'])) + '. Revisar formato datos, los IDs y los LBT deben ser enteros.')
    
    
    #Identificar los LBT_ID diferentes que hay en la red. LBT_ID = numeración única. LBT_NOMBRE = 1,2,3...
    LBT_ID_list = df_nodos_ct.loc[:,['LBT_ID','LBT_NOMBRE']].drop_duplicates(subset=['LBT_ID', 'LBT_NOMBRE'], keep = 'first').reset_index(drop=True)
    logger.debug('Lista de LBTs localizadas: ' + str(len(LBT_ID_list)))
    
    #Buscar el nodo correspondiente al CT. En el archivo Topología no está definido, pero en el de traza si.
    id_ct = df_nodos_ct.loc[:,['CT','CT_NOMBRE']].drop_duplicates(subset=['CT', 'CT_NOMBRE'], keep = 'first').reset_index(drop=True)['CT']#[0]    
    logger.debug('Listado de CTs identificados: ' + str(len(id_ct)))
    if len(id_ct) > 1:
        logger.error('Revisar el ID del CT. Se ha encontrado más de uno.')
    id_ct = id_ct[0]
       
    # Se genera el grafo y se lLama a la función que añade nodos y enlaces.
    #G = nx.DiGraph() #Grafo dirigido
    G = nx.Graph() #Grafo NO dirigido
    #N_nodo = len(df_nodos_ct)
    #print('No. de nodos: ' + str(N_nodo))
    G = self.genera_grafo(G, df_nodos_ct, df_traza_ct, LBT_ID_list, id_ct)
    
    #Identificar todos los nodos que tienen más de un predecesor-sucesor
    #Se hace antes de asociar los CUPS para que no devuelva los nodos de terminación de red una vez conectados los CUPS
    splitting_nodes_sin_cups = [node for node in G if G.degree(node)>=3]
    #Para cada splitting node se crea un campo que define el número de enlaces que tiene y otro que sirve como indicador de la agregación aguas abajo para más adelante
    for row in splitting_nodes_sin_cups:
        G.nodes[row]['Enlaces_orig'] = G.degree(row)
        G.nodes[row]['Enlaces_iter'] = G.degree(row)
        
    #Identificar los nodos finales de la red antes de asignar los CUPS
    end_nodes_sin_cups = [node for node in G if G.degree(node)==1]
    
    logger.debug('Encontrados ' + str(len(splitting_nodes_sin_cups)) + ' nodos con bifurcaciones y ' + str(len(end_nodes_sin_cups)) + ' nodos terminación de línea.')
    
    ##############################################################################
    ## Lectura de los archivos de CUPS y asociación de CUPS con el nodo correpsondiente del grafo.
    ##############################################################################
    df_cch = pd.DataFrame(columns=['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25', 'FLAG_H01', 'FLAG_H02', 'FLAG_H03', 'FLAG_H04', 'FLAG_H05', 'FLAG_H06', 'FLAG_H07', 'FLAG_H08', 'FLAG_H09', 'FLAG_H10', 'FLAG_H11', 'FLAG_H12', 'FLAG_H13', 'FLAG_H14', 'FLAG_H15', 'FLAG_H16', 'FLAG_H17', 'FLAG_H18', 'FLAG_H19', 'FLAG_H20', 'FLAG_H21', 'FLAG_H22', 'FLAG_H23', 'FLAG_H24', 'FLAG_H25'])
    df_cch_giss = pd.DataFrame(columns=['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25', 'FLAG_H01', 'FLAG_H02', 'FLAG_H03', 'FLAG_H04', 'FLAG_H05', 'FLAG_H06', 'FLAG_H07', 'FLAG_H08', 'FLAG_H09', 'FLAG_H10', 'FLAG_H11', 'FLAG_H12', 'FLAG_H13', 'FLAG_H14', 'FLAG_H15', 'FLAG_H16', 'FLAG_H17', 'FLAG_H18', 'FLAG_H19', 'FLAG_H20', 'FLAG_H21', 'FLAG_H22', 'FLAG_H23', 'FLAG_H24', 'FLAG_H25'])
    #Listar contenido carpeta curvas de carga
    contenido = os.listdir(self.ruta_cch)
    for i in contenido:
        palabra_clave = 'CAPTADA' #Palafra que identifica a los clientes. Para el CT es GISS
        if i.find(palabra_clave) >= 0:
            df_temp = []
            #Ahora se comprueba si el archivo es el que se corresponde con la fecha indicada
            if i.find(str(fecha)[0:6]) >= 0:
                try:
                    archivo_cch = self.ruta_cch + i
                    df_temp = pd.read_csv(archivo_cch, encoding='Latin9', header=0, sep=';', quotechar='\"')
                    df_cch = df_cch.append(df_temp, ignore_index=True).reset_index(drop=True)
                except:
                    logger.error('Error al leer el archivo con las curvas de carga de los clientes: ' + archivo_cch + '. Ejecución abortada.')
                    raise
        
        #Se busca también el archivo correspondiente a las medidas a la salida del CT
        palabra_clave_2 = 'AE_GISS'
        if i.find(palabra_clave_2) >= 0:
            if i.find(str(fecha)[0:6]) >= 0:
                #Lectura del archivo con las medidas a la salida del CT:
                try:
                    archivo_cch_giss = self.ruta_cch + i
                    df_cch_giss = pd.read_csv(archivo_cch_giss, encoding='Latin9', header=0, sep=';', quotechar='\"')
                except:
                    logger.error('Error al leer el archivo con las curvas de carga del CT: ' + archivo_cch_giss + '. Ejecución abortada.')
                    raise
        
    
    #Se eliminan duplicados al terminar. Se han localizado CUPS trifásicos que están el archivo de TF4 y en TF5 con las mismas fechas.
    df_cch = df_cch.drop_duplicates(keep = 'first').reset_index(drop=True)
    df_cch_giss = df_cch_giss.drop_duplicates(keep = 'first').reset_index(drop=True)
    
    if len(df_cch) == 0 or len(df_cch_giss) == 0:
        logger.error('Error al cargar las curvas de carga. Archivo df_cch con ' + str(len(df_cch)) + ' registros y archivo df_cch_giss con ' + str(len(df_cch)))
    
    df_P = df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'A')]
    df_P = df_P.append(df_cch[['CUPS', 'FECHA', 'MAGNITUD', 'DATA_VALIDATION', 'VALOR_H01', 'VALOR_H02', 'VALOR_H03', 'VALOR_H04', 'VALOR_H05', 'VALOR_H06', 'VALOR_H07', 'VALOR_H08', 'VALOR_H09', 'VALOR_H10', 'VALOR_H11', 'VALOR_H12', 'VALOR_H13', 'VALOR_H14', 'VALOR_H15', 'VALOR_H16', 'VALOR_H17', 'VALOR_H18', 'VALOR_H19', 'VALOR_H20', 'VALOR_H21', 'VALOR_H22', 'VALOR_H23', 'VALOR_H24', 'VALOR_H25']][(df_cch['MAGNITUD'] == 7) & (df_cch['DATA_VALIDATION'] == 'P')], ignore_index=True).reset_index(drop=True)
    df_P_fecha = df_P[df_P['FECHA'] == fecha]
    #Obtención de los CUPS únicos de la curva de carga
    CUPS_unicos = df_P_fecha.drop_duplicates(subset=['CUPS'], keep='last')['CUPS'].reset_index(drop=True)
    
    logger.debug('Encontrados ' + str(len(CUPS_unicos)) + ' CUPS únicos en los archivos de curvas de carga de clientes y ' + str(len(df_cch_giss)) + ' en el archivo de la curva de carga medida a la salida del CT.')
    
    #Se recorre el diccionario de horas para aplicar sobre el grafo los valores de potencia de cada hora por separado y hacer los cálculos.
    nodos_cups_conectados = [] #Se guardan todos los nodos que tienen al menos un CUP conectado y NO tienen otra rama con descendientes
    nodos_cups_descendientes = [] #Se guardan los nodos que tienen CUPs conectados y SI tienen ramas con descendientes.
    
    for clave in diccionario_horas.keys():
        colum_hora = clave     
        
        #Función para agregar los CUPS al grafo con el formato CUP_LBT_ID, a la arqueta compuesta correspondiente.
        #Devuelve el grafo, el listado de nodos con cups conectados SIN otras ramas, los que SI tienen otras ramas además de los CUPS y el listado de CUPS con el agregado del CT.
        G, nodos_cups_conectados, nodos_cups_descendientes, cups_agregado_CT = self.add_cups_grafo(G, colum_hora, CUPS_unicos, df_ct_cups_ct, df_matr_dist, df_P_fecha, id_ct, LBT_ID_list, nodos_cups_conectados, nodos_cups_descendientes)
        
        if self.save_plt_graph == 0:
            plt.subplot(111)
            pos = nx.get_node_attributes(G,'pos')
            #nx.draw(G, with_labels=True)
            #nx.draw(G, cmap = plt.get_cmap('jet'), with_labels=True, pos=nx.spring_layout(G))
            #nx.draw_networkx_edges(G, pos=nx.spring_layout(G))
            #nx.draw_networkx_nodes(G, pos=nx.spring_layout(G))
            nx.draw_kamada_kawai(G, node_size=7)
    #        nx.draw(G, pos=pos)
        #    plt.show(block=False)
            plt.savefig(plt_graph_file, format="JPG", dpi=300, bbox_inches='tight')
            plt.close()
            plt.subplot(111)
            nx.draw(G, pos=pos, node_size=7)
            plt.savefig(plt_graph_file_v2, format="JPG", dpi=300, bbox_inches='tight')
            plt.close()
        #    plt.draw()
    
        
        #Se crea una lista con todos los nodos que pueden ser terminación de línea y/o tener CUPs conectados
        end_nodes_cups = []
        end_nodes_cups = end_nodes_sin_cups + nodos_cups_conectados
        end_nodes_cups = list(dict.fromkeys(end_nodes_cups))
        #Posible caso del id_ct en la lista end_nodes. Hay que eliminarlo. Caso donde solo salga una línea del CT
        if id_ct in end_nodes_cups:
            end_nodes_cups.remove(id_ct)
        
        #Se elimina el id_ct de splitting nodes para no iterar sobre él.
        #No tendría que ser necesario eliminarlo
        if id_ct in splitting_nodes_sin_cups:
            splitting_nodes_sin_cups.remove(id_ct)
        
        ### Se calcula el agregado total de las curvas de carga
        #NO calcularlo al final, porque se sumarían dos veces las cargas que están dependiendo de los nodos etiquetados como 'nodos_cups_descendientes'. Ahora se evita eso
        P_R_carga_tot = 0
        Q_R_carga_tot = 0
        P_S_carga_tot = 0
        Q_S_carga_tot = 0
        P_T_carga_tot = 0
        Q_T_carga_tot = 0
        for row in end_nodes_cups:
            P_R_carga_tot += G.nodes[row]['P_R']
            Q_R_carga_tot += G.nodes[row]['Q_R']
            P_S_carga_tot += G.nodes[row]['P_S']
            Q_S_carga_tot += G.nodes[row]['Q_S']
            P_T_carga_tot += G.nodes[row]['P_T']
            Q_T_carga_tot += G.nodes[row]['Q_T']
        #    print(row, G.nodes[row]['P'], G.nodes[row]['Q'])
        ## IMPORTANTE: Al calcular la carga total conectada, sumar también los enlaces de la lista 'nodos_cups_descendientes', que son los cups conectados directamente a un nodo que ya tenía descendientes
        for row in nodos_cups_descendientes:
            P_R_carga_tot += G.nodes[row]['P_R']
            Q_R_carga_tot += G.nodes[row]['Q_R']
            P_S_carga_tot += G.nodes[row]['P_S']
            Q_S_carga_tot += G.nodes[row]['Q_S']
            P_T_carga_tot += G.nodes[row]['P_T']
            Q_T_carga_tot += G.nodes[row]['Q_T']
        #    print(row, G.nodes[row]['P'], G.nodes[row]['Q'])
        
        if P_R_carga_tot == 0 or P_S_carga_tot == 0 or P_T_carga_tot == 0:
            logger.warning('La potencia total agregada en los CUPS es es 0 en alguna de las fases (R, S, T): ' + str(P_R_carga_tot) + ', ' + str(P_S_carga_tot) + ', ' + str(P_T_carga_tot))
        
        #Se resuelve el grafo
        G = self.resuelve_grafo(G, end_nodes_cups, id_ct, splitting_nodes_sin_cups, self.temp_cables)
            
        ##############################################################################
        ## Obtención de los parámetros finales para el escenario definido.
        ##############################################################################
        # Pérdidas totales en los vanos
        P_R_vanos_tot = 0
        Q_R_vanos_tot = 0
        P_S_vanos_tot = 0
        Q_S_vanos_tot = 0
        P_T_vanos_tot = 0
        Q_T_vanos_tot = 0
        for n1, n2, data in G.edges(data = True, default = 0):
            #Hay que considerar que desde la última arqueta hasta el CUPS solo está definida la P y Q de la fase correspondiente, sin los try da error.
            try:
                P_R_vanos_tot += G.edges[n1, n2]['P_R_Linea']
                Q_R_vanos_tot += G.edges[n1, n2]['Q_R_Linea']
            except:
                pass
            try:
                P_S_vanos_tot += G.edges[n1, n2]['P_S_Linea']
                Q_S_vanos_tot += G.edges[n1, n2]['Q_S_Linea']
            except:
                pass
            try:
                P_T_vanos_tot += G.edges[n1, n2]['P_T_Linea']
                Q_T_vanos_tot += G.edges[n1, n2]['Q_T_Linea']
            except:
                pass
        
        if P_R_vanos_tot == 0 or P_S_vanos_tot == 0 or P_T_vanos_tot == 0:
            logger.warning('Las pérdidas totales en alguna de las fases es 0 (R, S, T): ' + str(P_R_vanos_tot) + ', ' + str(P_S_vanos_tot) + ', ' + str(P_T_vanos_tot))
        
        logger.debug('Cálculo realizado para: ' + self.Nombre_CT + ' ' + str(fecha) + ' ' + colum_hora)
        print('\n' + self.Nombre_CT + ' ' + str(fecha) + ' ' + colum_hora)
        print('Total pérdidas vanos (R, S, T): ', P_R_vanos_tot, P_S_vanos_tot, P_T_vanos_tot, ' kW, ', Q_R_vanos_tot, Q_S_vanos_tot, Q_T_vanos_tot, 'kVAr')
    
        #Agregado total curvas de carga
        print('Total cargas conectadas (R, S, T): ', P_R_carga_tot, P_S_carga_tot, P_T_carga_tot, ' kW, ', Q_R_carga_tot, Q_S_carga_tot, Q_T_carga_tot, 'kVAR')
        
        #Agregado total CT
        P_R_CT_tot = G.nodes[id_ct]['P_R']
        Q_R_CT_tot = G.nodes[id_ct]['Q_R']
        P_S_CT_tot = G.nodes[id_ct]['P_S']
        Q_S_CT_tot = G.nodes[id_ct]['Q_S']
        P_T_CT_tot = G.nodes[id_ct]['P_T']
        Q_T_CT_tot = G.nodes[id_ct]['Q_T']
        
        if P_R_CT_tot == 0 or P_S_CT_tot == 0 or P_T_CT_tot == 0:
            logger.warning('El agregado total en el CT para alguna de las fases es 0 (R, S, T): ' + str(P_R_CT_tot) + ', ' + str(P_S_CT_tot) + ', ' + str(P_T_CT_tot))
    
        print('Diferencia total agregado CT con total curvas de carga (R, S, T): ', P_R_CT_tot-P_R_carga_tot, P_S_CT_tot-P_S_carga_tot, P_T_CT_tot-P_T_carga_tot, 'kW', Q_R_CT_tot-Q_R_carga_tot, Q_S_CT_tot-Q_S_carga_tot, Q_T_CT_tot-Q_T_carga_tot, 'kVAr')
        
        #Obtención del dato de agregado total a la salida del CT:
        if len(df_cch_giss) > 0:
            P_medida_ct = df_cch_giss.loc[(df_cch_giss['CODIGO_LVC'].str.find(str(id_ct)) >= 0) & (df_cch_giss['FECHA'] == fecha)][colum_hora].reset_index(drop=True)#[0]   
            if len(P_medida_ct) > 1:
                logger.error('Revisar la curva de carga del CUPS con el agregado en el CT. Se ha encontrado más de un valor para la fecha y hora ' + str(fecha) + ' ' + colum_hora)
            P_medida_ct = P_medida_ct[0]
        else:
            P_medida_ct = 0

#        if len(P_medida_ct) > 1:
#            logger.error('Revisar la curva de carga del CUPS con el agregado en el CT. Se ha encontrado más de un valor para la fecha y hora ' + str(fecha) + ' ' + colum_hora)
#            P_medida_ct = P_medida_ct[0]
    
        print('Potencia medida a la salida del CT: ', str(P_medida_ct), ' KW.')
              
        #Al llegar a la hora 24 ya estamos en el día siguiente y se suma 1 a la fecha para guardar correctamente el valor.
        if colum_hora == 'VALOR_H24':
            self.fecha_datetime = self.fecha_datetime + datetime.timedelta(days=1)
            fecha = int(str(self.fecha_datetime.strftime("%Y")) + str(self.fecha_datetime.strftime("%m")) + str(self.fecha_datetime.strftime("%d")))
        #Identificador del caso para el SQL
        id_caso = int(str(fecha) + str(diccionario_horas.get(colum_hora)))
        
        
        ##############################################################################
        ## Guardado de datos en la BBDD SQL.
        ##############################################################################
        #Se define el nombre de dos de las tablas SQL, las que contendrán todos los datos del grafo
        tabla_ct_nodos = "OUTPUT_" + str(id_ct) + "_" + self.Nombre_CT + "_NODOS"
        tabla_ct_trazas = "OUTPUT_" + str(id_ct) + "_" + self.Nombre_CT + "_TRAZAS" 
        
        if (self.guardado_bbdd == 0) or (self.guardado_bbdd == 1) or (self.guardado_bbdd == 2):
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
    
        if (self.guardado_bbdd == 0) or (self.guardado_bbdd == 2):
            #Se recorren todos los nodos y se guardan en el SQL las P y Q calculadas.
            for nodo, data in G.nodes(data=True, default = 0):
                #Se guarda la información de los nodos, sin contar los CUPS. Los CUPS darían error porque solo tienen P y Q de una fase (si son monofásicos)
                if data['Tipo_Nodo'] != 'CUP':
                    try:
                        instruccion_insert = "INSERT INTO " + tabla_ct_nodos + " (ID_Caso, ID_NODO_LBT_ID, Fecha, Hora, P_R_KW, Q_R_KVAR, P_S_KW, Q_S_KVAR, P_T_KW, Q_T_KVAR) VALUES (" + str(id_caso) + ", '" + str(nodo) + "', '" + str(fecha) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(data['P_R'])) + ", " + str(float(data['Q_R'])) + ", " + str(float(data['P_S'])) + ", " + str(float(data['Q_S'])) + ", " + str(float(data['P_T'])) + ", " + str(float(data['Q_T'])) + ");"
                        cursor.execute(instruccion_insert)
                        conn.commit()
                    except:
                        logger.error('Error al guardar en la BBDD. ' + instruccion_insert)
                        continue
                    
            for nodo1, nodo2, data in G.edges(data = True, default = 0):
                #Se guarda la información de las trazas, sin contar los enlaces con los CUPS. Los CUPS darían error porque solo tienen P y Q de una fase (si son monofásicos)
                if (G.nodes[nodo1]['Tipo_Nodo'] != 'CUP') and (G.nodes[nodo2]['Tipo_Nodo'] != 'CUP'):
                    try:
                        instruccion_insert = "INSERT INTO " + tabla_ct_trazas + " (ID_Caso, ID_NODO_LBT_ID_INI, ID_NODO_LBT_ID_FIN, Fecha, Hora, P_R_LINEA_KW, Q_R_LINEA_KVAR, P_S_LINEA_KW, Q_S_LINEA_KVAR, P_T_LINEA_KW, Q_T_LINEA_KVAR) VALUES (" + str(id_caso) + ", '" + str(nodo1) + "', '" + str(nodo2) + "', '" + str(fecha) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(data['P_R_Linea'])) + ", " + str(float(data['Q_R_Linea'])) + ", " + str(float(data['P_S_Linea'])) + ", " + str(float(data['Q_S_Linea'])) + ", " + str(float(data['P_T_Linea'])) + ", " + str(float(data['Q_T_Linea'])) + ");"
    #                   print(instruccion_insert)
                        cursor.execute(instruccion_insert)
                        conn.commit()
                    except:
                        logger.error('Error al guardar en la BBDD. ' + instruccion_insert)
                        continue
            
        
        if (self.guardado_bbdd == 0) or (self.guardado_bbdd == 1):
            try:
                instruccion_insert = "INSERT INTO " + self.tabla_cts_general + " (ID_Caso, ID_CT, CT_NOBRE, Fecha, Hora, P_R_CT_KW, Q_R_CT_KVAR, P_S_CT_KW, Q_S_CT_KVAR,P_T_CT_KW, Q_T_CT_KVAR, P_CT_MEDIDO_KW, Q_CT_MEDIDO_KVAR, P_R_LINEAS_KW, Q_R_LINEAS_KVAR, P_S_LINEAS_KW, Q_S_LINEAS_KVAR, P_T_LINEAS_KW, Q_T_LINEAS_KVAR) VALUES (" + str(id_caso) + ", " + str(id_ct) + ", '" + self.Nombre_CT + "', '" + str(fecha) + "', '" + diccionario_horas.get(colum_hora) + ":00:00" + "', " + str(float(P_R_CT_tot)) + ", " + str(float(Q_R_CT_tot)) + ", " + str(float(P_S_CT_tot)) + ", " + str(float(Q_S_CT_tot)) + ", " + str(float(P_T_CT_tot)) + ", " + str(float(Q_T_CT_tot)) + ", " + str(float(P_medida_ct)) + ", 0, " + str(float(P_R_vanos_tot)) + ", " + str(float(Q_R_vanos_tot)) + ", " + str(float(P_S_vanos_tot)) + ", " + str(float(Q_S_vanos_tot)) + ", " + str(float(P_T_vanos_tot)) + ", " + str(float(Q_T_vanos_tot)) + ");"
                cursor.execute(instruccion_insert)
                conn.commit()
            except:
                logger.error('Error al guardar en la BBDD. ' + instruccion_insert)
                continue

        if (self.guardado_bbdd == 0):
            logger.debug(str(colum_hora) + ' guardado correctamente en la BBDD en todas las tablas.')
        elif (self.guardado_bbdd == 1):
            logger.debug(str(colum_hora) + ' guardado correctamente en la BBDD, pero únicamente en la tabla de agregados CT: ' + self.tabla_cts_general)
        elif (self.guardado_bbdd == 2):
            logger.debug(str(colum_hora) + ' guardado correctamente en la BBDD, pero únicamente en las tablas de cada CT con todos los datos del grafo: ' + tabla_ct_nodos + ', ' + tabla_ct_trazas + '. NO EN LA TABLA GENERAL DE AGREGADO CT: ' + self.tabla_cts_general)
        if (self.guardado_bbdd >= 3) or (self.guardado_bbdd < 0):
            logger.warning('Ningún dato guardado en la BBDD. Cambiar variable "guardado_bbdd" para guardar.')
            
    if (self.guardado_bbdd == 0) or (self.guardado_bbdd == 1) or (self.guardado_bbdd == 2):
        cursor.close()
        del cursor
    
    logger.info('Fin de la ejecución: ' + str(time.strftime("%d/%m/%y")) + ' a las ' + str(time.strftime("%H:%M:%S")))
    logger.info('###################################################################')
    



    
             
            