import requests
import pandas as pd
import re,time,asyncio,random
from tyrone_mings import *
from typing import List

#Jugadores ya scrapeados
df1 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory1.xlsx')
df2 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory2.xlsx')
df3 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory3.xlsx')
df4 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory4.xlsx')
df5 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory5.xlsx')
df6 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory6.xlsx')
df7 = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\Auxiliares\transfermarktHistory7.xlsx')

dfUnion = pd.concat([df1, df2, df3, df4,df5,df6,df7], ignore_index=True)
dfUnion.to_excel("transfermarktHistoryDefinitivo.xlsx", index = False)

print(len(dfUnion.index))
dfUnion = dfUnion.drop_duplicates(subset=['Link'])
list1 = list(dfUnion['Link'].values.tolist())
print(len(list1))

#Enlaces de todos los jugadores
dfEnlaces = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-ExtraccionPrevia\Transfermarkt\Excels/linkJugadores.xlsx')
listEnlaces = list(dfEnlaces['LinkJugadores'].values.tolist())

#Ver que enlaces no se han scrapeado aun
print("Entrada a restantes")
pruned = []
for element in listEnlaces:
        if element not in [link for link in dfUnion['Link']]:
            pruned.append(element)
print(len(pruned))
dfEnlacesRestantes = pd.DataFrame(pruned, columns = ['Link'])

dfEnlacesRestantes.to_excel("enlacesRestantes.xlsx", index=False)