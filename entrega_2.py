import requests
import json
import prettytable
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# connect to the BLS API to download inflation data 

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

# connect to the db

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

# Create the new "date" column by combining "year" and "period"
df['date'] = '01-' + df['period'].str[1:] + '-' + df['year']

# Convert the "date" column to datetime format
df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
df['date'] = df['date'].dt.strftime('%d-%m-%Y')

# Remove the "year" and "period" columns
df = df.drop(['year', 'period'], axis=1)

df = df.rename(columns={'series id': 'series_id'})
df['series_id'] = df['series_id'].astype(str)
df['value'] = df['value'].astype(float)
df['date'] = df['date'].astype(str)

def check_null_values(df):
    if df.isnull().values.any():
        print("There are null or missing values in the DataFrame.")
    else:
        print("No null or missing values found in the DataFrame.")

check_null_values(df)

def cargar_en_redshift(conn, table_name, df):
    dtypes = df.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'object': 'VARCHAR(50)', 'float64': 'FLOAT'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]

    # Define format SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]

    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
    """

    # Create table
    cur = conn.cursor()
    cur.execute(table_schema)

    # Get the existing data in the table
    cur.execute(f"SELECT {', '.join(cols)} FROM {table_name}")
    existing_data = cur.fetchall()

    # Convert the existing data to a set for faster lookup
    existing_data_set = set(existing_data)

    # Filter out duplicate rows from the DataFrame
    unique_rows = df[~df[cols].apply(tuple, axis=1).isin(existing_data_set)]

    # Get the number of rows uploaded
    rows_uploaded = len(unique_rows)

    if rows_uploaded > 0:
        # Convert the filtered data to a list of tuples
        values = [tuple(x) for x in unique_rows.to_numpy()]

        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"

        # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")

    # Get the number of rows not uploaded due to duplication
    rows_not_uploaded = len(df) - rows_uploaded

    # Print the results
    print(f"Rows uploaded: {rows_uploaded}")
    print(f"Rows not uploaded (duplicates): {rows_not_uploaded}")

    cur.close()
    print('Process finished')

cargar_en_redshift(conn=conn, table_name='BLS', df=df)