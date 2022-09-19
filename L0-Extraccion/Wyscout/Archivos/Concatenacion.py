import re
import pandas as pd
import numpy as np
import glob


path="C:/Users/carlo/Desktop/Proyecto/Excels/Excels brutos"

file_list = glob.glob(path +"/*.xlsx")

#Recorremos cada excel de Wyscout y lo unimos a un dataframe
excel_list = []

for file in file_list:
    excel_list.append(pd.read_excel(file))

excel_merged = pd.DataFrame()

for excel_file in excel_list:
    excel_merged = excel_merged.append(excel_file,ignore_index=True)

cols = excel_merged.select_dtypes(include=[object]).columns


#Quitamos todos los carácteres especiales de valores y colummnas
excel_merged[cols] = excel_merged[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
excel_merged.columns = excel_merged.columns.str.normalize('NFKD').str.encode('ascii',errors='ignore').str.decode('utf-8')

#Dividimos posiciones en tres y pasaporte en 2. Reemplazamos valores de posicion
excel_merged.rename(columns = {'Pasaporte':'NacionalidadPrincipal','Posicion especifica':'PosicionEspecifica'}, inplace=True)

excel_merged['PosicionEspecifica'].replace({'GK':'POR','LB':'LI','LCB':'DFCI','CB':'DFC','RCB':'DFD','RB':'LD','RWB':'CAD',
'LWB':'CAI','LDMF':'MCDI','DMF':'MCD','RDMF':'MCDD','LW':'MI','LCMF':'MCI','RCMF':'MC','RW':'MD','LAMF':'MPI',
'AMF':'MP','RAMF':'MPD','LWF':'EI','CF':'DC','RWF':'ED'}, inplace=True)

excel_merged[['Posicion 1', 'Posicion 2', 'Posicion 3']] = excel_merged.PosicionEspecifica.str.split(',', expand=True)
excel_merged[['Nacionalidad 1', 'Nacionalidad 2']] = excel_merged.NacionalidadPrincipal.str.split(',', expand=True)
excel_merged = excel_merged.drop(['PosicionEspecifica','NacionalidadPrincipal'], axis=1)

#Cambiamos el orden de algunas columnas(Inf general y luego datos numéricos)
posicion1 = excel_merged['Posicion 1']
posicion2 = excel_merged['Posicion 2']
posicion3 = excel_merged['Posicion 3']
nac1 = excel_merged['Nacionalidad 1']
nac2 = excel_merged['Nacionalidad 2']
pai = excel_merged['Pais de nacimiento']
pie = excel_merged['Pie']
altur = excel_merged['Altura']
peso = excel_merged['Peso']
enp = excel_merged['En prestamo']
excel_merged = excel_merged.drop(columns=['Posicion 1','Posicion 2','Posicion 3','Nacionalidad 1','Nacionalidad 2','Pais de nacimiento',
'Pie','Altura','Peso','En prestamo'])
excel_merged.insert(loc=4, column='Posicion 1', value=posicion1)
excel_merged.insert(loc=5, column='Posicion 2', value=posicion2)
excel_merged.insert(loc=6, column='Posicion 3', value=posicion3)
excel_merged.insert(loc=7, column='Pais de nacimiento', value=pai)
excel_merged.insert(loc=8, column='Nacionalidad 1', value=nac1)
excel_merged.insert(loc=9, column='Nacionalidad 2', value=nac2)
excel_merged.insert(loc=10, column='Pie', value=pie)
excel_merged.insert(loc=11, column='Altura', value=altur)
excel_merged.insert(loc=12, column='Peso', value=peso)
excel_merged.insert(loc=13, column='En prestamo', value=enp)

#Formatos de columnas con la primera mayúscula de cada palabra y quitamos espacios
excel_merged.columns = excel_merged.columns.str.replace(r'(\w+)', lambda x: x.group().capitalize(),n=5, regex=True)
excel_merged.columns = excel_merged.columns.str.replace(' ', '')

excel_merged.reset_index(drop=True)
excel_merged.to_excel("jugadoresWyscout.xlsx", index=False)