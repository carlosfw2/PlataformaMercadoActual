from base64 import encode
import requests,random,re,sys,time
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime

#Quitar espacios blancos manual Excel con Reemplazar. No reemplaza todos los valores blanco utilizando replace.

df = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-Extraccion\Transfermarkt\Excels\transfermarktHistory.xlsx')

#Quitar columna Link
#df = df.drop(['Link'],axis=1)

#Reemplazar th por K y m por M a MV y Fee
df.iloc[:,5].replace({'Th.':'K','m':'M'}, inplace=True,regex=True)
df.iloc[:,6].replace({'Th.':'K','m':'M','\?':'-'}, inplace=True,regex=True)

#Cambiar formato fecha de %b %d,%Y a %Y-%m-%d a Date
format1 ="%b %d, %Y"
format2 = "%Y-%m-%d"

df.iloc[:,2]= df.iloc[:,2].apply(lambda x: datetime.strptime(x, format1).strftime(format2) if x != "-" else x)

df.to_excel("transfermarktHistory.xlsx",index= False)



