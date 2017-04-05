
import pandas as pd
import kmedoids 
import numpy as np

#matriz_distancia = pd.DataFrame.from_csv('matrices/matriz_distancia.csv')
#matriz_distancia = pd.DataFrame.from_csv('matrices/Test-Medium1K/matriz_distancia1k.csv')
#matriz_distancia = pd.DataFrame.from_csv('matrices/Test-Largest1K/matriz_distancia1k.csv')
matriz_distancia = pd.DataFrame.from_csv('matrices/matriz_distancia1k_largest.csv')

for x in range(1, 101):
    print "Muestra %d" % (x)


    df_objetos = matriz_distancia[['obj1']]


    ran_obj = df_objetos.take(np.random.permutation(len(df_objetos))[:100])

    objetos_selecion =[]
    for obj in ran_obj.values.tolist():
        objetos_selecion.append(obj[0])
    
    print("Seleccion de objetos")
    #subconjunto de la matriz con 1000 objetos
    seleccion = matriz_distancia.loc[matriz_distancia['obj1'].isin(objetos_selecion)]


    seleccion = seleccion[['obj1']+objetos_selecion].reset_index()
    del seleccion['index']

    df_objetos = seleccion[['obj1']]
    del seleccion['obj1']

    objetos = df_objetos.values

    D = seleccion.values
    # split into 10 clusters
    M, C = kmedoids.kMedoids(D, 10)

    lista_kmedoids =[]

    print('medoids:')
    for point_idx in M:
        print( objetos[point_idx] )
        lista_kmedoids.append(objetos[point_idx])
    
    df = pd.DataFrame(lista_kmedoids)
    df.columns = ['objeto']
    df.to_csv("clusters/1k/kmedoids_"+str(x)+".csv")


    results =[]
    for label in C:
        for point_idx in C[label]:
            results.append([str(label), objetos[point_idx][0]])


    df = pd.DataFrame(results)
    df.columns = ['cluster','objeto']
    df.to_csv("clusters/1k/cluster_results_"+str(x)+".csv")



#cluster para el universo

#cargar datos matriz de distancia




#matriz_distancia = pd.DataFrame.from_csv('matrices/matriz_distancia.csv')
df_objetos = matriz_distancia[['obj1']]
del matriz_distancia['obj1']
objetos = df_objetos.values

D = matriz_distancia.values
print(D)
# split into 10 clusters
M, C = kmedoids.kMedoids(D, 10)
print(M)
lista_kmedoids =[]

print('medoids:')

for point_idx in M:
    print( objetos[point_idx] )
    lista_kmedoids.append(objetos[point_idx])

df = pd.DataFrame(lista_kmedoids)
df.columns = ['objeto']
df.to_csv("clusters/1k/kmedoids.csv")


results =[]
for label in C:
    for point_idx in C[label]:
        results.append([str(label), objetos[point_idx][0]])


df = pd.DataFrame(results)
df.columns = ['cluster','objeto']
df.to_csv("clusters/1k/cluster_results.csv")

