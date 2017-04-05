import numpy as np
import pandas as pd

from scipy.spatial.distance import euclidean
from fastdtw import fastdtw
import os
import time

def calcular_distancia(obj1, obj2):
    files_path = "/home/gcarrillo/proyecto/ClusterTimeSeries-1000/trace-Opens.txt-interarrivals-1000"
       
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

      
    distancia =0

    if (obj1 == obj2):
        distancia =0
    else:
        #distancia =1
        distancia, path = fastdtw(np.asarray(arr_serie1),np.asarray(arr_serie2), dist=euclidean)

    return distancia         
#cargar archivo con nombres de los objetos

def genera_matriz(objetos, matriz):
    for i in range(len(objetos)):
        for x in range(i+1, len(objetos)):
            distancia = calcular_distancia(objetos[i], objetos[x]);
            
            dict = {'obj1':  objetos[i], 'obj2':  objetos[x],'distancia':distancia}

            matriz.append(dict)


fileName = '/home/gcarrillo/proyecto/lista_archivos.txt'        
lista_objetos =[x for x in (line.strip() for line in open(fileName, 'r'))]

startTimeQuery = time.time()

valores_matriz = []
genera_matriz(lista_objetos,valores_matriz)
endTimeQuery = time.time()


print("Tiempo de ejecucion para calculo de distancia entre objetos: %f segundos." % (endTimeQuery - startTimeQuery))

df_matriz = pd.DataFrame(valores_matriz)


df_matriz_inv = df_matriz[['obj2','obj1','distancia']]
df_matriz_inv.columns = ['obj1','obj2','distancia']
    
frames  = [df_matriz, df_matriz_inv]
df_result = pd.concat(frames).reset_index()
del df_result['index']
   
print("registros completados")

df_clean = df_result.drop_duplicates(subset=['obj1', 'obj2','distancia'])
    
dff = df_clean.pivot(index='obj1', columns='obj2', values='distancia').reset_index()



print("fin")