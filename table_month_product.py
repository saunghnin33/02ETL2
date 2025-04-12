import pandas as pd
import psycopg2
# from tqdm import tqdm


# Configuration 
excel_file_path = r"D:\Saung Hnin Phyu\02ETL\month1\2014_01_product.xlsx"

table_name='monthlysales.d_product';
db_config={
    'host':'localhost',
    'port':'5432',
    'dbname':'monthlysales',
    'user':'postgres',
    'password':'admin'
}

# load data
df = pd.read_excel(excel_file_path)

# connect to pgsql
conn=psycopg2.connect(**db_config);
cur=conn.cursor();

# Insert data
for i,row in df.iterrows():
    columns=','.join(df.columns)
    values=','.join(['%s']*len(row))
    insert_query=f'INSERT INTO {table_name}({columns}) VALUES({values})'
    cur.execute(insert_query,tuple(row))

# commit and close
conn.commit();
cur.close();
conn.close();

print("data loaded successfully")
