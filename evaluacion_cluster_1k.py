
import pandas as pd
import os
import numpy as np
from scipy.spatial.distance import euclidean

from fastdtw import fastdtw
import time

startTimeQuery = time.time()
#matriz_distancia = pd.DataFrame.from_csv('matrices/matriz_distancia.csv')
#matriz_distancia = pd.DataFrame.from_csv('matrices/Test-Medium1K/matriz_distancia1k.csv')
#matriz_distancia = pd.DataFrame.from_csv('matrices/Test-Largest1K/matriz_distancia1k.csv')
matriz_distancia = pd.DataFrame.from_csv('matrices/matriz_distancia1k_largest.csv')


df_objetos = matriz_distancia[['obj1']]
valores_matriz = pd.DataFrame.copy(matriz_distancia)
del valores_matriz['obj1']
objetos = df_objetos.values.tolist()
D = valores_matriz.values
endTimeQuery = time.time()    
    
    
print("Tiempo de carga de matriz : %f segundos." % (endTimeQuery - startTimeQuery))
    
    
#cargar los medoids del universo
kmedoids = pd.DataFrame.from_csv('clusters/1k/kmedoids.csv')
clusters_universo = pd.DataFrame.from_csv('clusters/1k/cluster_results.csv')
objetos_universo = kmedoids.values.tolist()

lista_acc = []
#cargar los medoids de la muestra 
for x in range(1, 101):
    print "Muestra %d" % (x)
    startTimeQuery = time.time()
    kmedoids_1 = pd.DataFrame.from_csv('clusters/1k/kmedoids_' + str(x)+'.csv')
    
    objetos_muestra = kmedoids_1.values.tolist()

    #hacer nuevo cluster con los medoids de la muestra
    
    nuevos_indices = []
    for obj in objetos_muestra:
        nuevos_indices.append(objetos.index([obj[0]]))
    
    M =np.asarray(nuevos_indices)

    #NUEVO CLUSTER
    C = {}
    k=10
        # determine clusters, i. e. arrays of data indices
    J = np.argmin(D[:,M], axis=1)
    for kappa in range(k):
            C[kappa] = np.where(J==kappa)[0]        
    
    
    
    cluster_results =[]
    for label in C:
        for point_idx in C[label]:
            #cluster_results.append([lista_kmedoids[label][0], objetos[point_idx][0]])
            cluster_results.append([label, objetos[point_idx][0]])
    
    
    clusters_muestra = pd.DataFrame(cluster_results)
    clusters_muestra.columns = ['cluster','objeto']
    #clusters_muestra.to_csv("clusters/10k/cluster_results_nuevo_" + str(x)+".csv")

    #FIN NUEVO CLUSTER
    

    
    resultados = []
    

    #por cada uno de los  cluster de la muestra ver cuanto de sus miembros están en el cluster dle universo
    pares = []
    relacion_clusters = []
    for y in range(10):
        #print("-----Cluster" + str(y))
        startTimeQuery = time.time()
        
        objetos_cluster = clusters_muestra.loc[clusters_muestra['cluster']==y]

        objetos_cluster_muestra = objetos_cluster['objeto'].values.tolist()
        objetos_presentes = clusters_universo.loc[clusters_universo['objeto'].isin(objetos_cluster_muestra)]
        objetos_presentes_counts = objetos_presentes.groupby('cluster').size().reset_index();
        objetos_presentes_counts.columns = ['cluster','cuenta']
        objetos_presentes_counts_ordered = objetos_presentes_counts.sort(['cuenta'], ascending=[0])
        #cuento por cada cluster cuantos hay
        #print(objetos_presentes)
        #print("xxxxx")

        objetos_presentes_counts_ordered["cluster_muestra"] = y
        objetos_presentes_counts_ordered["cluster_muestra_cuenta"] = len(objetos_cluster)
        ids_clusters = objetos_presentes_counts_ordered["cluster"].values.tolist()

        #agregar los indices que faltan
        for z in range(10):
            if z not in ids_clusters :
                
                datav=[[z,0,y,len(objetos_cluster)]]
                
                relacion_clusters = relacion_clusters + datav
        relacion_clusters = relacion_clusters + objetos_presentes_counts_ordered.values.tolist()




    df_relacion_clusters = pd.DataFrame(relacion_clusters)
    df_relacion_clusters.columns = ['cluster','cuenta','cluster_muestra','total']


    df_relacion_clusters_ordered = df_relacion_clusters.sort(['cuenta','total'], ascending=[0,0])

    #hacer los pares en base al total de match    
    #esta lista para saber cuales ya he seleccionado
    seleccionados = []
    seleccionados_muestra = []
    total_aciertos=[]
    #recorrer todo el dataframe
    for index, row in df_relacion_clusters_ordered.iterrows():

        if row['cluster_muestra'] not in seleccionados_muestra :
            if row['cluster'] not in seleccionados :
                seleccionados_muestra.append(row['cluster_muestra'])
                seleccionados.append(row['cluster'])
                total_aciertos.append(row['cuenta'])
                #aqui el par
                pares.append([row['cluster'],row['cluster_muestra']])
            
    

    resultados = []

    for par in pares:

        resultados.append([objetos_universo[par[0]][0],objetos_muestra[par[1]][0]])
    #si no están todos los pares completar

#    print("Resultados, pares")
#    print(resultados)
    acc = (sum(total_aciertos)/(len(objetos)*1.0))*100
    lista_acc.append(acc)
    
    print("Accuracy:")
    print(acc)
    endTimeQuery = time.time()    
    
    
df_acc = pd.DataFrame(lista_acc)
df_acc.to_csv("clusters/1k/acc.csv")
    #print("Tiempo analisis nuevo cluster : %f segundos." % (endTimeQuery - startTimeQuery))
#guardar los acc en archivo
acc_ave = (sum(lista_acc)/(10*1.0))  
print("Accuracy Avg:")
print(acc_ave)