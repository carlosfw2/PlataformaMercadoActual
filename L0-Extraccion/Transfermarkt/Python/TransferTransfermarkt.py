import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
from tyrone_mings import *
import time
from multiprocessing import Pool,cpu_count
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import List
import random
from requests_ip_rotator import ApiGateway, EXTRA_REGIONS

pd.set_option('notebook_repr_html', True)
pd.options.mode.chained_assignment = None

df = pd.read_excel(r'C:\Users\carlo\Desktop\Proyecto\L0-ExtraccionPrevia\Transfermarkt\Excels/enlacesRestantes.xlsx')
list1 = list(df['Link'].values.tolist())

nameList = []
transfer_history = pd.DataFrame(columns=["Link","Player","Season", "Date", "Left", "Joined", "MV", "Fee"])
transfer_history2 = pd.DataFrame(columns=["Link","Player","Season", "Date", "Left", "Joined", "MV", "Fee"])
gateway = ApiGateway("https://www.transfermarkt.com",access_key_id = "AKIAT5DZSBQO7XJQH7FF", access_key_secret = "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")
gateway.start()

def getInfo(link: str,session):
    global transfer_history,transfer_history2
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
    else:
        name = data_header_el.getText().split('\n')[-1].strip()
        print(name.encode(),len(nameList))
    nameList.append(name) 

    #TransferHistory
    rows = soup.find_all("div", {"class": "tm-player-transfer-history-grid"})
    reached_prev_transfers = True # Assume we've reached past transfers
    for row in rows[1:-1]:
        fields = [s.strip() for s in row.getText().split("\n\n") if s!=""]
        if "Upcoming transfer" in fields:
            # The next row will be a future transfer
            reached_prev_transfers = False
        if reached_prev_transfers:
            new_row = pd.Series({
                "Link": link,
                "Player": name,
                "Season": fields[0],
                "Date":   fields[1],
                "Left":   fields[2],
                "Joined": fields[3],
                "MV":     fields[4],
                "Fee":    fields[5]
            })
            transfer_history = transfer_history.append(new_row, ignore_index=True)
        if "Transfer history" in fields:
            # Now we've reached past transfers
                reached_prev_transfers = True
    return transfer_history

async def getInfo_async(links: List[str]) -> list:
    res = []
    with ThreadPoolExecutor(max_workers=24) as executor:
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

cols = transfer_history.select_dtypes(include=[object]).columns
transfer_history[cols] = transfer_history[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
transfer_history.to_excel('transfermarktHistory8.xlsx', index=False)
