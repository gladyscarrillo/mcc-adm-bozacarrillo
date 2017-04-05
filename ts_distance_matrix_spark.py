
from __future__ import print_function
import os
import sys
from operator import add
from pyspark import SparkContext

from pyspark.sql.types import *
from pyspark.sql.functions import array, col, explode, struct, lit
from pprint import pprint
from pyspark.mllib.linalg import Vectors

import time
import pandas as pd
import os
import math
import numpy as np

from scipy.spatial import distance_matrix



from scipy.spatial.distance import cityblock
from scipy.spatial.distance import euclidean

from fastdtw import fastdtw

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    # Euclidean distance.
    def euc_dist(pt1,pt2):
        return math.sqrt((pt2[0]-pt1[0])*(pt2[0]-pt1[0])+(pt2[1]-pt1[1])*(pt2[1]-pt1[1]))
    
    def _c(ca,i,j,P,Q):
        if ca[i,j] > -1:
            return ca[i,j]
        elif i == 0 and j == 0:
            ca[i,j] = euc_dist(P[0],Q[0])
        elif i > 0 and j == 0:
            ca[i,j] = max(_c(ca,i-1,0,P,Q),euc_dist(P[i],Q[0]))
        elif i == 0 and j > 0:
            ca[i,j] = max(_c(ca,0,j-1,P,Q),euc_dist(P[0],Q[j]))
        elif i > 0 and j > 0:
            ca[i,j] = max(min(_c(ca,i-1,j,P,Q),_c(ca,i-1,j-1,P,Q),_c(ca,i,j-1,P,Q)),euc_dist(P[i],Q[j]))
        else:
            ca[i,j] = float("inf")
        return ca[i,j]
    
    """ Computes the discrete frechet distance between two polygonal lines
    Algorithm: http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
    P and Q are arrays of 2-element arrays (points)
    """
    def frechetDist(P,Q):
        sys.setrecursionlimit(50000)
        ca = np.ones((len(P),len(Q)))
        ca = np.multiply(ca,-1)
        return _c(ca,len(P)-1,len(Q)-1,P,Q)
    
    def cargar_tiempos(objeto):

        #files_path = "/home/ayudante-abad/EdwinBoza/ADM/proyecto/ClusterTimeSeries-1000/trace-Opens.txt-interarrivals-1000"
        #files_path = "/home/ayudante-abad/EdwinBoza/ADM/proyecto/trace-Opens.txt-interarrivals"
        files_path = "/home/ayudante-abad/EdwinBoza/ADM/proyecto/trace-Opens.txt-interarrivals-10klargest"
	#files_path= "/home/gcarrillo/proyecto/Test-Largest1K/trace-Opens.txt-interarrivals-10klargest/"
        nombre_objeto = "it-" + objeto + ".txt"
        
        serie = os.path.join(files_path,nombre_objeto)
        tiempos =[float(x) for x in (line.strip() for line in open(serie, 'r'))]

        return (objeto, tiempos)

    def calcular_distancia(registro):
        #leer archivo con la distancia
        obj1 = registro[0]
        obj2 = registro[1]
        
	#files_path= "/home/gcarrillo/proyecto/Test-Largest1K/trace-Opens.txt-interarrivals-10klargest/"
        files_path = "/home/ayudante-abad/EdwinBoza/ADM/proyecto/trace-Opens.txt-interarrivals-10klargest"

        #tiempos de objeto 1
        nombre_objeto = "it-" + obj1 + ".txt"
        
        serie = os.path.join(files_path,nombre_objeto)
        serie1 =[float(x) for x in (line.strip() for line in open(serie, 'r'))]

        #tiempos de objeto 2
        nombre_objeto = "it-" + obj2 + ".txt"
        
        serie = os.path.join(files_path,nombre_objeto)
        serie2 =[float(x) for x in (line.strip() for line in open(serie, 'r'))]
        

        arr_serie1 = []
        arr_serie2 = []
        #print(serie1)

        for i,x in enumerate(serie1):
            tupla = [i,x]
            arr_serie1.append(tupla)
            #result_array = np.append(result_array, [tupla], axis=0)

            
        for i,x in enumerate(serie2):
            tupla = [i,x]
            arr_serie2.append(tupla)       

        #print(result_array)
        #distancia =  discrete_frechet(np.asarray(arr_serie1),np.asarray(arr_serie2))

        distancia =0
        if (registro[0] == registro[1]):
            distancia =0
            #print("-")
        else:
            #print("x")
            distancia =0
            #distancia =cityblock(arr1,arr2)
            distancia, path = fastdtw(np.asarray(arr_serie1),np.asarray(arr_serie2), dist=euclidean)
            #distancia = frechetDist(arr_serie1,arr_serie2)

        return (registro[0], registro[1],distancia)   


        
    
    sc = SparkContext('local[*]','adm-proyecto') 

    #RUTA DEL ARCHIVO

    srcfileName = os.getcwd()+'/trace-Opens.txt-10klargest-onlyIDs'
    fileName = os.getcwd()+'/lista_objetos.txt'
    matrizfileName = os.getcwd()+'/matriz_distancia.csv'
 
    os.system('shuf ' +srcfileName +'| head -n 10000 > ' + fileName )
  
    #cargar archivo con los datos
    startTimeQuery = time.time()
    objetos = sc.textFile(fileName,48)

    endTimeQuery = time.time()

    print("Tiempo de ejecucion para la carga de nombre de los objetos: %f segundos." % (endTimeQuery - startTimeQuery))

    startTimeQuery = time.time()
    
   

    pairs_data = objetos.cartesian(objetos).filter(lambda (a, b): a <= b)    
    
    pairs_data2 = sc.parallelize(pairs_data.collect(),48)
  
#    print(pairs_data.count())
#    data2 = pairs.cartesian(data)
    
    #print(pairs_data.take(25))
    endTimeQuery = time.time()
    ##print("Tiempo de inicio: %f" % startTimeQuery)
    ##print("Tiempo de fin: %f" % endTimeQuery)
    print("Tiempo de combinar objetos: %f segundos." % (endTimeQuery - startTimeQuery))
    
    
    #calcular la distancia
    startTimeQuery = time.time()
    matriz_distancia = pairs_data2.map(calcular_distancia).collect()
    df_matriz =pd.DataFrame(matriz_distancia, columns=['obj1','obj2','distance']) 
    #completar matriz
    df_matriz_inv = df_matriz[['obj2','obj1','distance']]
    df_matriz_inv.columns = ['obj1','obj2','distance']
    
    df_matriz.to_csv(matrizfileName)
    frames  = [df_matriz, df_matriz_inv]
    df_result = pd.concat(frames).reset_index()
    del df_result['index']
   
    #df_rep = df_result.loc[df_result['obj1']==df_result['obj2']]    

    df_clean = df_result.drop_duplicates(subset=['obj1', 'obj2','distance'])
    
    dff = df_clean.pivot(index='obj1', columns='obj2', values='distance').reset_index()
    
    print("xxxxxxxxxxxxxxxxxxxx")
    #print(dff.values)
    dff.to_csv(matrizfileName)
    #print(matriz_distancia[0:20])
    
    #print(matriz_distancia.take(25))
    endTimeQuery = time.time()
    print("Tiempo de calcular la distancia : %f segundos." % (endTimeQuery - startTimeQuery))
