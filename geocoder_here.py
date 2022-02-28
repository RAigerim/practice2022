import sqlalchemy
import geocoder_here
import pandas as pd
from geopy.geocoders import HereV7
import numpy as np
from time import sleep

app_code = ''
app_id = ''
server_name = ''
table_name = ''

conn = sqlalchemy.create_engine(
    f'mssql+pyodbc://' + server_name + '/kazakhstan?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')

q = """select b.id, b.number, lower(t.value_ru) + ' ' + g.name_rus as name_rus
            from b_kyzylorda b 
            left join geonims g on g.id = b.s_geonim_id
            left join geonims_types t on g.d_geonims_type_id = t.id
            where g.s_ats_id not in (168004, 165288)
            order by b.id"""

df = pd.read_sql_query(q, conn)

dtype = {
    'id': sqlalchemy.types.NVARCHAR(),
    'address': sqlalchemy.types.NVARCHAR(),
    'resultType': sqlalchemy.types.NVARCHAR(),
    'countryCode': sqlalchemy.types.NVARCHAR(),
    'county': sqlalchemy.types.NVARCHAR(),
    'city': sqlalchemy.types.NVARCHAR(),
    'district': sqlalchemy.types.NVARCHAR(),
    'street': sqlalchemy.types.NVARCHAR(),
    'houseNumber': sqlalchemy.types.NVARCHAR(),
    'lat': sqlalchemy.types.NVARCHAR(),
    'lng': sqlalchemy.types.NVARCHAR(),
    'queryScore': sqlalchemy.types.NVARCHAR(),
    'streetScore': sqlalchemy.types.NVARCHAR(),
    'houseScore': sqlalchemy.types.NVARCHAR()
}

h = HereV7(apikey=app_code)


def geocoding(i, df):
    g = h.geocode(components={'country': 'казахстан',
                              'city': 'кармакчинский',
                              'district': 'байконур',
                              'street': df.loc[i, 'name_rus'],
                              'houseNumber': str(df.loc[i, 'number'])},
                  countries=['KAZ'], language='rus', timeout=60)
    return g.raw if pd.notnull(g) else np.nan


cols = ['id', 'address', 'resultType', 'countryCode', 'county', 'city', 'district', 'street', 'houseNumber',
        'lat', 'lng', 'queryScore', 'streetScore', 'houseScore']

for i in list(df.index):
    geocode = geocoding(i, df)
    geocoded_df = pd.DataFrame(columns=cols, dtype='object')
    geocoded_df.loc[i, 'id'] = df.loc[i, 'id']
    if pd.isnull(geocode):
        geocoded_df.astype(str).to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype)
        print(i, geocoded_df.loc[i, 'id'])
        continue
    geocoded_df.loc[i, 'address'] = geocode['title']
    geocoded_df.loc[i, 'resultType'] = geocode['resultType']
    geocoded_df.loc[i, 'countryCode'] = geocode['address']['countryCode']
    geocoded_df.loc[i, 'county'] = geocode['address']['county']
    if 'city' in geocode['address'].keys():
        geocoded_df.loc[i, 'city'] = geocode['address']['city']
    if 'district' in geocode['address'].keys():
        geocoded_df.loc[i, 'district'] = geocode['address']['district']
    if 'street' in geocode['address'].keys():
        geocoded_df.loc[i, 'street'] = geocode['address']['street']
    if 'houseNumber' in geocode['address'].keys():
        geocoded_df.loc[i, 'houseNumber'] = geocode['address']['houseNumber']
    geocoded_df.loc[i, 'lat'] = geocode['position']['lat']
    geocoded_df.loc[i, 'lng'] = geocode['position']['lng']
    geocoded_df.loc[i, 'queryScore'] = geocode['scoring']['queryScore']
    if 'streets' in geocode['scoring']['fieldScore'].keys():
        geocoded_df.loc[i, 'streetScore'] = geocode['scoring']['fieldScore']['streets'][0]
    if 'houseNumber' in geocode['scoring']['fieldScore'].keys():
        geocoded_df.loc[i, 'houseScore'] = geocode['scoring']['fieldScore']['houseNumber']
    geocoded_df.astype(str).to_sql(table_name, conn, if_exists='append', index=False, dtype=dtype)
    print(i, geocoded_df.loc[i, 'id'])
