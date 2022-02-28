import requests
import json
import pandas as pd
import sqlalchemy
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from time import sleep
import warnings

warnings.filterwarnings("ignore")

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

headers = {
    'Host': 'data.egov.kz',
    'User-Agent': """Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0 Safari/537.36""",
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cookie': """lttping=1624524813693; _ga=GA1.2.119799985.1611131318; _ym_uid=16111313181057905347;_ym_d=1611131318; egovLang=ru; OPENDATA_PORTAL_SESSION=f0ef007816d35fcffd1ba6c7acbeda4803c47ed1-___AT=7974c2a8f0e554ef0b9822ffe585b67823bf7a85&___TS=1624528417591; cookiesession1=678B76A8HIOPQRSTUVWXYZABCEFGEF57; _gid=GA1.2.1616680045.1624524465; _ym_isad=1; _ym_visorc=w""",
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}

conn = sqlalchemy.create_engine(
    f"""mssql+pyodbc://LAPTOP-0A3PAI5G\SQLEXPRESS/kazakhstan?charset=utf8&trusted_connection=yes&driver=ODBC Driver 17 for SQL Server""",
    encoding='utf8')

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

query = """select id from ats where cato like '63%'"""
table_name = 'grounds_VKO'
sqldf = pd.read_sql_query(query, conn)
# sqldf = sqldf[sqldf.index > 769]
# sqldf = pd.read_csv(table_name + '.txt', names=['id'])

for ats_id in sqldf.id:
    for i in range(100):

        url = 'https://data.egov.kz/api/v4/s_grounds_new/v2?apiKey=a8747d8d66b947108ab1001f2777deaa&source={%22from%22:' + str(
            i * 2000) + ',%22size%22:2000,%22query%22:{%22bool%22:{%22must%22:[{%22match%22:{%22s_ats_id%22:%22' + str(
            ats_id) + '%22}}]}}}'
        response = session.get(url, verify=False, headers=headers)
        if response.status_code == 404:
            response = requests.get(url, verify=False, headers=headers)
        try:
            rjson = response.json()
        except json.decoder.JSONDecodeError:
            with open(table_name + '.txt', 'a') as file:
                file.write(str(ats_id) + '\n')
            print('JSONDecodeError\n' + str(sqldf[sqldf.id == ats_id].id))
            break
        data = pd.DataFrame(rjson)
        data.to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype)
        print(str(sqldf[sqldf.id == ats_id].id) + '\t' + str(len(data)))
        sleep(3)
        if len(data) < 2000:
            break
