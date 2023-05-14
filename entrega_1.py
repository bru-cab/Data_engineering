import requests
import json
import prettytable
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# conectar a la API del BLS para descargar la data de inflacion de US

headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUUR0000SA0',],"startyear":"2013", "endyear":"2023"})
p = requests.post('https://api.bls.gov/publicAPI/v1/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)

df = pd.DataFrame(columns=["series id","year","period","value"])

for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        if 'M01' <= period <= 'M12':
            x.add_row([seriesId,year,period,value])
            df = df.append({"series id": seriesId,
                            "year": year,
                            "period": period,
                            "value": value}, ignore_index=True)
    output = open(seriesId + '.txt','w')
    output.write (x.get_string())
    output.close()

print(df.head())

# conexion a la base de datos

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="cabrerabru_coderhouse"
with open("C:/Users/cabre/Documents/Economics/Cursos/Data engineering/pwd_coder.txt",'r') as f:
    pwd= f.read()

try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname='data-engineer-database',
        user='cabrerabru_coderhouse',
        password='gD0qPM42CM',
        port='5439'
    )
    print("Connected to Redshift successfully!")
    
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)

# ETL

print(df.dtypes)

df = df.rename(columns={'series id': 'series_id'})

df['series_id'] = df['series_id'].astype(str)
df['year'] = df['year'].astype(float)
df['value'] = df['value'].astype(float)
df['period'] = df['period'].astype(str)

def cargar_en_redshift(conn, table_name, dataframe):
    dtypes= df.dtypes
    cols= list(dtypes.index )
    tipos= list(dtypes.values)
    type_map = {'object': 'VARCHAR(50)','float64': 'FLOAT','object': 'VARCHAR(50)','float64': 'FLOAT'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    # Crear la tabla
    cur = conn.cursor()
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in df.to_numpy()]
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')
     
cargar_en_redshift(conn=conn, table_name='BLS', dataframe=df)








