import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re,sys
from tyrone_mings import *
import time
from multiprocessing import Pool
import tqdm

pd.set_option('notebook_repr_html', True)
pd.options.mode.chained_assignment = None

#En el FOR ponemos las páginas que queremos captar de Transfermarkt
leagueList = []

leaguesToKeep = ['AR1N','ARG2','B1AP','BRA1','BRA2','CLPD','COL1','CRPD','EL1A','FR2','FR3','CN2A','CN2B','CN2C','CN2D',
'L2','L3','GB3','GB4','GU1A','HO1A','IT2','IT3A','IT3B','IT3C','IT4A','IT4B','IT4C','IT4D','IT4E','IT4F','IT4G','IT4H'
,'IT4I','MEX1','MEXB','MAR1','NL1','NL2','SA1','PR1A','RSK1','TDeA','PO2','SFA1','ES2','E3G1','E3G2','E4G1','E4G2','E4G3',
'E4G4','E4G5','VZ1L','URU1','URU2','BE1','BE2','A1','A2','ZYP1','AUS1','KR1','TS1','TS2','DK1','DK2','FI1','FI2','GR1','GRS2',
'IS1','UNG','JAP1','JAP2','LET1','LI1','MO1N','NO1','NO2','PL1','PL2','RU1','RU2','SC1','SC2','SER1','SLO1','SL1','SE1',
'SE2','C1','C2','TR1','MLS1','USL','ES1','GB1','L1','FR1','IT1','GB2','PO1','TR2','UKR1','UKR2','RO1','RO2','UNG1']


def getObtenerLigas(urlContinente, numeroPags):
    for page in range(1,numeroPags):
        r = requests.get(urlContinente,
            params= {"page": page},
            headers= {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0"}
        )
        pageSoup = bs(r.content, "html.parser")

    #Añadimos cada uno de los enlaces de las ligas 
        for a_href in pageSoup.find_all("a", href=True):
            leagueList.append(str(a_href["href"]))


    df = pd.DataFrame(leagueList)
    df.columns = ["Link"]


    #Se filtra por el contenido de la url y el regex final

    df = df[df["Link"].str.contains("startseite/wettbewerb/" 
    + "[A-Za-z0-9]{2,5}$")==True].reset_index(drop=True)

    #Cogemos la sigla de cada liga por la última posición de la url y añadimos un boolean para ver las que están en la lista inicial.
    df['Sigla'] = df['Link'].str.split("/").str[-1]
    df['TrueFalse'] = 0
    for i in df.index:
        if df['Sigla'][i] in leaguesToKeep:
            df["TrueFalse"][i] = 1

    df2=df.query("TrueFalse == 1")
    df2 = df2.drop(['Sigla','TrueFalse'],axis= 1)

    leagueList.clear()
    return df2


dfEurope = getObtenerLigas("https://www.transfermarkt.com/wettbewerbe/europa",8)
dfAsia = getObtenerLigas("https://www.transfermarkt.com/wettbewerbe/asien",3)
dfAmerika = getObtenerLigas("https://www.transfermarkt.com/wettbewerbe/amerika",3)
dfAfrika = getObtenerLigas("https://www.transfermarkt.com/wettbewerbe/afrika",2)

frames = [dfEurope, dfAsia, dfAmerika, dfAfrika]
df = pd.concat(frames)
dict = {'Link': '/egyptian-premier-league/startseite/wettbewerb/EGY1'}
df = df.append(dict, ignore_index = True)
df.to_excel("ligas.xlsx", index=False)

#Paso 2: URLs de los jugadores a partir de ligas con libreria Tyrone_Mings

df2 = pd.DataFrame()

#Recorremos cada uno de los enlaces.
#   Si no se ha introducido ninguna liga, se crea la columna con los jugadores de la primera liga
#   Si ya hay una liga, se añaden los jugadores del resto de ligas a las ligas añadidas.

def getJugadoresURL(df,df2):
    playersLink = []
    players2Link = []
    for i in df.index:
        playersLink = get_player_urls_from_league_page("https://transfermarkt.com" + df["Link"][i] + "/plus/?saison_id=2021", verbose=True)
        if i==0:
            df2["LinkJugadores"] = playersLink
        else:
            players2Link= list(set(playersLink)-set(df2['LinkJugadores']))
            df2 = df2.append(pd.DataFrame({'LinkJugadores': players2Link}), ignore_index=True) 

        players2Link.clear()
        playersLink.clear()
        print(i)
#Se reemplazan los enlaces que haya de mercado por los de perfiles y se esconde el indice.

    df2 = df2.replace('marktwertverlauf','profil', regex=True)
    df2 = df2.drop_duplicates()

    df2.to_excel("linkJugadores.xlsx", index= False)
    return df2

dfJugadores = getJugadoresURL(df,df2)
