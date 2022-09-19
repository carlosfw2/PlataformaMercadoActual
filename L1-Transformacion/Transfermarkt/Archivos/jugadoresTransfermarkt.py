from base64 import encode
import requests,random,re,sys,time
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime

#Quitar espacios blancos manual Excel con Reemplazar. No reemplaza todos los valores blanco utilizando replace.
df = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\jugadoresTransfermarkt.xlsx')

#Quitar el indice del valor de cada columna
df.iloc[:,0]= df.iloc[:,0].str.split('.').str[1]
df.iloc[:,1]= df.iloc[:,1].str.split('.').str[1]
df.iloc[:,2]= df.iloc[:,2].str.split('.').str[1]
df.iloc[:,3]= df.iloc[:,3].str.split('.').str[1]
df.iloc[:,4]= df.iloc[:,4].str.split('.').str[1]
df.iloc[:,5]= df.iloc[:,5].str.split('.').str[1]
df.iloc[:,6]= df.iloc[:,6].str.split('.').str[1]
df.iloc[:,7]= df.iloc[:,7].str.split('.').str[1]
df.iloc[:,8]= df.iloc[:,8].str.split('.').str[1]
df.iloc[:,9]= df.iloc[:,9].str.split('.').str[1]
df.iloc[:,10]= df.iloc[:,10].str.split('.').str[1]
df.iloc[:,11]= df.iloc[:,11].str.split('.').str[1]
df.iloc[:,12]= df.iloc[:,12].str.split('.').str[1]
df.iloc[:,13]= df.iloc[:,13].str.split('.').str[1]
df.iloc[:,14]= df.iloc[:,14].str.split('.').str[1]

#Cambiar todos los None por -
df.replace({"None":"-","N/A  ":"-"},inplace=True,regex=True)

#Cambiar formato fecha de %b %d,%Y a %Y-%m-%d a FechaNacimiento, InicioContrato y FinContrato
format1 ="%b %d, %Y"
format2 = "%Y-%m-%d"

df.iloc[:,1]= df.iloc[:,1].apply(lambda x: datetime.strptime(x, format1).strftime(format2) if (x != "-") and (len(x)>=11) else x)
df.iloc[:,13] = df.iloc[:,13].apply(lambda x: datetime.strptime(x, format1).strftime(format2) if (x != "-") and (len(x)>=11) else x)
df.iloc[:,14] = df.iloc[:,14].apply(lambda x: datetime.strptime(x, format1).strftime(format2) if (x != "-") and (len(x)>=11) else x)


df.to_excel("jugadoresTransfermarkt.xlsx",index= False)