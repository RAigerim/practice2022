import requests
import json
import pandas as pd
import sqlalchemy
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from time import sleep
import warnings

warnings.filterwarnings("ignore")

server_name = ''
api_key = ''

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

headers = {
    'Host': 'data.egov.kz',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cookie': 'lttping=1624524813693; _ga=GA1.2.119799985.1611131318; _ym_uid=16111313181057905347; _ym_d=1611131318; egovLang=ru; OPENDATA_PORTAL_SESSION=f0ef007816d35fcffd1ba6c7acbeda4803c47ed1-___AT=7974c2a8f0e554ef0b9822ffe585b67823bf7a85&___TS=1624528417591; cookiesession1=678B76A8HIOPQRSTUVWXYZABCEFGEF57; _gid=GA1.2.1616680045.1624524465; _ym_isad=1; _ym_visorc=w',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}

conn = sqlalchemy.create_engine(
    f'mssql+pyodbc://'+server_name+'/kazakhstan?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')

dtype = {
    'full_path_kaz': sqlalchemy.types.NVARCHAR(),
    'old_path_kaz': sqlalchemy.types.NVARCHAR(),
    'name_kaz': sqlalchemy.types.NVARCHAR(),
    'name_rus': sqlalchemy.types.NVARCHAR(),
    'full_path_rus': sqlalchemy.types.NVARCHAR(),
    'old_path_rus': sqlalchemy.types.NVARCHAR(),
    'modified': sqlalchemy.types.NVARCHAR(),
    'id': sqlalchemy.types.NVARCHAR(),
    'd_room_type_id': sqlalchemy.types.NVARCHAR(),
    'category_room': sqlalchemy.types.NVARCHAR(),
    'd_room_type_code': sqlalchemy.types.NVARCHAR(),
    's_building_id': sqlalchemy.types.NVARCHAR(),
    'actual': sqlalchemy.types.NVARCHAR(),
    'number': sqlalchemy.types.NVARCHAR(),
    'rca': sqlalchemy.types.NVARCHAR(),
    'cadastre_number': sqlalchemy.types.NVARCHAR(),
    's_ats_id': sqlalchemy.types.NVARCHAR(),
    'rca_building': sqlalchemy.types.NVARCHAR()
}

query = """select id from b_nursultan"""
table_name = 'pb_nursultan'
# sqldf = pd.read_csv(table_name+'.txt', names=['id'])
sqldf = pd.read_sql_query(query, conn)
# sqldf = sqldf[sqldf.index > 93310]


for buildings_id in sqldf.id:

    url = 'https://data.egov.kz/api/v4/s_pb/v3?apiKey='+api_key+'&source={%22from%22:0,%22size%22:3000,%22query%22:{%22bool%22:{%22must%22:[{%22match%22:{%22s_building_id%22:%22' + str(
        buildings_id) + '%22}}]}}}'
    response = session.get(url, verify=False, headers=headers)
    if response.status_code == 404:
        response = requests.get(url, verify=False, headers=headers)
    try:
        rjson = response.json()
    except json.decoder.JSONDecodeError:
        with open(table_name+'.txt', 'a') as file:
            file.write(str(buildings_id) + '\n')
        print('JSONDecodeError\n' + str(sqldf[sqldf.id == buildings_id].id))
        continue
    if len(rjson) == 0:
        # print(str(sqldf[sqldf.id == buildings_id].id) + '\t' + str(len(rjson)))
        continue
    data = pd.DataFrame(rjson)
    data.to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype)
    print(str(sqldf[sqldf.id == buildings_id].id) + '\t' + str(len(data)))
    sleep(3)
