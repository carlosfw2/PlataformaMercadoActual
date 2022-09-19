from base64 import encode
import requests,random
from bs4 import BeautifulSoup as bs
import pandas as pd
import re,sys
from tyrone_mings import *
import time
from concurrent.futures import ThreadPoolExecutor
import asyncio,aiohttp
from typing import List
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS

t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
print(current_time)

pd.set_option('notebook_repr_html', True)
pd.options.mode.chained_assignment = None

df = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-ExtraccionPrevia\Transfermarkt\Excels/linkJugadores.xlsx')
list1 = list(df['LinkJugadores'].values.tolist())

#Sacamos la info de cada jugador
noList,imList,na1List,na2List,na3List,biList,agList,heList,poList,otList,coList,exList,teList,mvList,leList = [],[],[],[],[],[],[],[],[],[],[],[],[],[],[]

gateway = ApiGateway("https://www.transfermarkt.com",access_key_id = "XXXX", access_key_secret = "XXXXXXXXX")
gateway.start()

def getInfo(link: str,session):
    time.sleep(random.uniform(1,2.5))
    user_agent_list = [
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.38 Safari/537.36 Brave/75', 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.38 Safari/537.36 Brave/75', 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36 Brave/18', 
	'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Brave Chrome/80.0.3987.87 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Brave Chrome/89.0.4389.114 Safari/537.36' 
]
    user_agent = user_agent_list[random.randint(0, len(user_agent_list)-1)]
    headers = {'User-Agent': user_agent} 
    tree = session.get(link, headers=headers,timeout=120)
    soup = BeautifulSoup(tree.content, 'html.parser')

    #Nombre jugador
    data_header_el = soup.find("h1", {"class": "data-header__headline-wrapper"})
    if not data_header_el:
        name = None
        name = str(len(noList)) + "." + str(name)
    else:
        name = data_header_el.getText().split('\n')[-1].strip()
        print(name.encode(),len(noList))
        name = str(len(noList)) + "." + name
    noList.append(name)
    pos = noList.index(name)

    #Contract & Team
    joined = str(pos) + "." + str(None)
    contract_expires = str(pos) + "." + str(None)
    team = soup.find("span", {"class": "data-header__club"})
    if (team is not None) and (team.find('a') is not None):
        team = str(pos) + "." + team.find('a')["title"]
    else:
        team = str(pos) + "." + str(None)
    league = soup.find("span", {"class": "data-header__league"})
    if(league is not None) and (league.find("img") is not None):
        league = str(pos) + "." + league.find("img")["title"]
    else:
        league = str(pos) + "." + str(None)
    if team in ["Without Club", "None","Retired","-",""," ",None]:
        joined = str(pos) + "." + str(None)
        contract_expires = str(pos) + "." + str(None)
    else:
        joined_list = []
        joinedAll = soup.find_all("span", {"class": "data-header__label"})
        if joinedAll is not None:
            for el in joinedAll:
                if "joined" in el.text.lower():
                    joined_list.append(el.text.split(": ")[-1])
            if len(joined_list) > 0:
                joined = str(pos) + "." + joined_list[0]
        else:
            joined = str(pos) + "." + str(None)
    #if (joined is not None) and (joined != "-"):
        #joined = datetime.datetime.strptime(joined, '%b %d, %Y').strftime('%Y-%m-%d')

    contract_list = []
    contractAll = soup.find_all("span", {"class": "data-header__label"})
    if contractAll is not None:
        for el in contractAll:
            if "expires" in el.text.lower():
                contract_list.append(el.text.split(": ")[-1])
        if len(contract_list) > 0:
            contract_expires = str(pos) + "." + contract_list[0]
    else:
        contract_expires = str(pos) + "." + str(None)
    #if (contract_expires is not None) and (contract_expires != "-"):
        #contract_expires = datetime.datetime.strptime(contract_expires, '%b %d, %Y').strftime('%Y-%m-%d')
    leList.append(league)
    coList.append(joined)
    exList.append(contract_expires)
    teList.append(team)
    

    #Position
    try:
        position = str(pos) + "." + soup.find("dd", {"class": "detail-position__position"}).getText()
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        position = str(pos) + "." + str(None)
    try:
        other_positions = str(pos) + "." + soup.find("div", {"class": "detail-position__position"}).find("dd").getText()
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        other_positions = str(pos) + "." + str(None)
    poList.append(position)
    otList.append(other_positions)

    #Nacionalidades
    try:
        citizenship_els = soup.find_all("span", {"class": "info-table__content info-table__content--bold"})
        flag_els = [flag_el for el in citizenship_els\
        for flag_el in el.find_all("img", {"class": "flaggenrahmen"})]
        citizenship = list(set([el["title"] for el in flag_els]))
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        citizenship = str(pos) + "." + str(None)

    if(len(citizenship) == 0):
        na1List.append(str(pos) + "." + str(None))
        na2List.append(str(pos) + "." + str(None))
        na3List.append(str(pos) + "." + str(None))
    if(len(citizenship) == 1):
        na1List.append(str(pos) + "." + str(citizenship[0]))
        na2List.append(str(pos) + "." + str(None))
        na3List.append(str(pos) + "." + str(None))
    if(len(citizenship) == 2):
        na1List.append(str(pos) + "." + str(citizenship[0]))
        na2List.append(str(pos) + "." + str(citizenship[1]))
        na3List.append(str(pos) + "." + str(None))
    if(len(citizenship) == 3):
        na1List.append(str(pos) + "." + str(citizenship[0]))
        na2List.append(str(pos) + "." + str(citizenship[1]))
        na3List.append(str(pos) + "." + str(citizenship[2]))

    #Imagen
    image =soup.find_all("img", {"class": "data-header__profile-image"})
    try:
        src = str(pos) + "." + image[0].get('src').split("?lm")[0]
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        src = str(pos) + "." + str(None)
    imList.append(src)

    #Fecha Nacimiento y edad  
    dob_el = soup.find("span", {"itemprop": "birthDate"})
    try:
        dob = str(pos) + "." + ' '.join(dob_el.getText().strip().split(' ')[:3])
        #dob = datetime.datetime.strptime(dob, '%b %d, %Y').strftime('%Y-%m-%d')
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        dob = str(pos) + "." + str(None)
    biList.append(dob)
    try:
        age = str(pos) + "." + str(int(dob_el.getText().strip().split(' ')[-1].replace('(','').replace(')','')))
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        age = str(pos) + "." + str(None)
    agList.append(age)

    #Altura
    height_el = soup.find("span", {"itemprop": "height"})
    try:
        height_meters = str(pos) + "." + str(height_el.getText().replace(" ","").replace("m", "").replace(".", ","))
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        height_meters = str(pos) + "." + str(None)
    heList.append(height_meters)

    #MarketValue
    mv = soup.find("div", {"class": "tm-player-market-value-development__current-value"})
    try:
        mv2 = str(pos) + "." + mv.getText().replace("Th.", "K").replace("m", "M").replace(" ","").replace("\n","").replace(".",",")
    except (AttributeError,IndexError,TypeError,ValueError,RuntimeError,OSError):
        mv2 = str(pos) + "." + str(None)
    mvList.append(mv2)
    return noList,biList,imList,teList,mvList,agList,heList,na1List,na2List,na3List,poList,otList,coList,exList

async def getInfo_async(links: List[str]) -> list:
    res = []
    with ThreadPoolExecutor(max_workers=48) as executor:
        with requests.Session() as session:
            session.mount("https://www.transfermarkt.com", gateway)
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor,getInfo, *(link,session)) for link in links
                ]
            for response in await asyncio.gather(*tasks):
                res.append(response)
    await asyncio.sleep((random.uniform(1,2.5)))
    return res

loop = asyncio.get_event_loop()
future = asyncio.ensure_future(getInfo_async(list1))
res = loop.run_until_complete(future)

#Creamos el dataframe resultanto con los datos en cada una de las listas.
df2 = pd.DataFrame(list(zip(noList, biList,imList,teList,leList,mvList,agList,na1List,na2List,na3List,heList,
                    poList,otList,coList,exList)),
            columns =['Nombre', 'FechaNacimiento','Imagen','Equipo','Liga','VM','Edad','Nacionalidad1','Nacionalidad2','Nacionalidad3','Altura','Posicion','PosicionSecundaria','InicioContrato','FinContrato'])

#Cambiamos la nomenclatura de las posiciones
df2['Posicion'].replace({'Goalkeeper':'GK','Left-Back':'LI','Defensive Midfield':'MCD'
,'Right Midfield':'MD','Attacking Midfield':'MP','Right Winger':'ED','Centre-Forward':'DC',
'Centre-Back':'DFC','Right-Back':'LD','Central Midfield':'MC','Second Striker':'SD',
'Left Midfield':'MI','Left Winger':'EI','N':'','None':'','Sweeper':'DFC','one':'None'}, inplace=True,regex=True)

df2['PosicionSecundaria'].replace({'Goalkeeper':'GK','Left-Back':'LI','Defensive Midfield':'MCD'
,'Right Midfield':'MD','Attacking Midfield':'MP','Right Winger':'ED','Centre-Forward':'DC',
'Centre-Back':'DFC','Right-Back':'LD','Central Midfield':'MC','Second Striker':'SD',
'Left Midfield':'MI','Left Winger':'EI','N':'','Sweeper':'DFC','one':'None'}, inplace=True,regex=True)

#df4 = pd.DataFrame(df2['Nacionalidad'].values.tolist(), columns = ['Nacionalidad1','Nacionalidad2','Nacionalidad3'])
#dfPrincipal = pd.concat([df2,df4], axis = 1)
#dfPrincipal = dfPrincipal.drop(['Nacionalidad'], axis=1)
dfPrincipal = df2[df2['Equipo'] != "Retired"]

df2['Nacionalidad1'].replace([''])

cols = dfPrincipal.select_dtypes(include=[object]).columns
#Quitamos todos los carácteres especiales de valores y colummnas
dfPrincipal[cols] = dfPrincipal[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
dfPrincipal[cols] = dfPrincipal[cols].replace("-","").replace("?","")

dfPrincipal = dfPrincipal.apply(lambda col: col.sort_values(key=lambda col: col.str.split(".").str[0].astype(int)).to_numpy())
dfPrincipal.to_excel('jugadoresTransfermarktPrueba.xlsx', index=False)