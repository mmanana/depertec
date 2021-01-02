#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    File name: cable.py
    Author: Mario Mañana, David Carriles, GTEA
    Date created: 28 Nov 2020
    Date last modified: 31 Dec 2020
    Python Version: 3.8
    
    Library for Line Loss Analysis and Calculation of Electric Power Systems
'''

import numpy as np
import scipy.integrate as integrate
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Wedge, Polygon, Ellipse
from matplotlib.collections import PatchCollection
import xml.etree.cElementTree as ET


class Conductor:
    # Rdc # DC resistance of conductor at the ambient temperature T0 [Ohms/km]
    # T0  # Ambient temperature T0
    # T1  # Ambient temperature T1
    # K1  # Skin effect coefficient of conductor
    # X1  # Parameter used to calculate the skin effect coefficient of conductor
    # Do  # Outer diameter of conductor
    # Di  # Inner diameter of conductor
    # f   # Frequency of AC current
    # K2  # Iron loss coefficient of conductor
    # X2  # Parameter used to calculate the iron loss coefficient of conductor
    # I   # Current
    # S   # Cross-sectional area of conductor [mm2]
    # Rac0 # AC resistance of conductor at the temperature T0 [Ohms/km]
    # Rac1 # AC resistance of conductor at the temperature T1 [Ohms/km]
    
    version = r'Line Loss Analysis Library. v0.05'
    Rdc: float
    T0: float
    T1: float
    K1: float
    X1: float
    Do: float
    Di: float
    f: float
    K2: float
    X2: float
    I: float
    S: float
    Rac0: float
    Rac1: float
    NameConductor: str
    LibraryConductor = r'cable_library.xml'
    
    
    def __init__( self, Rdc=0.2, Do=10, Di=1, f=50, I=0, S=10, T0=20.0, T1=20.0, alpha=0.004):
        self.Rdc = Rdc
        self.Do = Do
        self.Di = Di
        self.f = f
        self.I = I
        self.S = S
        self.alpha = alpha
        self.T0 = T0
        self.T1 = T1
        
    def fset_rdc( self, Rdc):
        self.Rdc = Rdc
        
    def fset_do( self, Do):
        self.Do = Do
        
    def fset_di( self, Di):
        self.Di = Di
        
    def fset_f( self, f):
        self.f = f
        
    def fset_i( self, I):
        self.I = I
        
    def fset_s( self, S):
        self.S = S

    def fset_t0( self, T0):
        self.T0 = T0
        
    def fset_t1( self, T1):
        self.T1 = T1
        
    def fget_do( self):
        return( self.Do)
    
    def fget_di( self):
        return( self.Di)
    
    def fget_f( self):
        return( self.f)

    def fget_i( self):
        return( self.I)

    def fget_s( self):
        return( self.S)
    
    def fget_t0( self):
        return( self.T0)

    def fget_t1( self):
        return( self.T1)
    
    def fget_rac0( self):
        return( self.Rac0)
    
    def fget_rac1( self):
        return( self.Rac1)
    
    def fget_version( self):
        return( self.version)
    
    def fcompute_r( self):
        self.X1 = ((self.Do+2*self.Di)/(self.Do+self.Di))*0.01*np.sqrt((8* \
                    np.pi*self.f*(self.Do-self.Di))/(self.Rdc*(self.Do+self.Di)))
        self.X2 = self.I/self.S
        print( str(self.X2))
        self.K1 = 0.99609+0.018578*self.X1-0.030263*self.X1*self.X1+0.020735* \
                    self.X1*self.X1*self.X1
        self.K2 = 0.99947+0.028895*self.X2-0.005934*self.X2*self.X2+0.00042259* \
                    self.X2*self.X2*self.X2
        self.Rac0 = self.K1*self.K2*self.Rdc
        self.Rac1 = self.Rac0*(1+self.alpha*(self.T1 - self.T0))
        return(self.Rac1)
    
    def fload_library( self, NameConductor):
        self.NameConductor = NameConductor
        
        cable_library = ET.ElementTree( file=r'./cable_library.xml')
        cable_library_root = cable_library.getroot()

        for children in cable_library_root:
        #print( childrem.tag, childrem.attrib)
            if children.attrib["name"]==self.NameConductor:
                print("+++++++++++++++++++++++++++++++")
                print(children.attrib["name"])
                for child in children:
                    print( child.tag, child.text)
                    if child.tag == "Rdc":
                        self.Rdc = float(child.text)
                    elif child.tag == "T0":
                        self.T0 = float(child.text)
                    elif child.tag == "Di":
                        self.Di = float(child.text)
                    elif child.tag == "Do":
                        self.Do = float(child.text)
                    elif child.tag == "S":
                        self.S = float(child.text)