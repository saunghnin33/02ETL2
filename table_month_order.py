import pandas as pd
import psycopg2
from tqdm import tqdm

excel_file_path=r"D:\Saung Hnin Phyu\02ETL\monthly_sales\sales_2016-12.xlsx"

order_table='monthlysales.f_order'
customer_table='monthlysales.d_customer'
location_table='monthlysales.d_location'
product_table='monthlysales.d_product'
db_config={
    'host':'localhost',
    'port':'5432',
    'dbname':'monthlysales',
    'user':'postgres',
    'password':'admin'
}


df = pd.read_excel(excel_file_path)

required_column=['order_id','order_date','ship_date','shipmode','customer_id','postal_code','product_id','sales','quantity','discount','profit']
dff=df[required_column].copy()

dff['order_date'] = pd.to_datetime(df['order_date'], errors='coerce');
dff['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce');


conn=psycopg2.connect(**db_config);
cur=conn.cursor();

for i,row in tqdm(dff.iterrows()):
    columns=','.join(dff.columns)
    values=','.join(['%s']*len(row))
    insert_query=f'INSERT INTO {order_table}({columns}) VALUES({values})'
    cur.execute(insert_query,tuple(row))

customer_columns=['customer_id','customer_name','segment']
customer_df=df[customer_columns].copy()

for i,row in tqdm(customer_df.iterrows()):
    columns=','.join(customer_df.columns)
    values=','.join(['%s']*len(row))
    insert_query = f"""INSERT INTO {customer_table} ({columns}) VALUES ({values})
                       ON CONFLICT (customer_id) DO NOTHING;;"""
    cur.execute(insert_query,tuple(row))

product_columns=['product_id','product_name','sub_category','category']
product_df=df[product_columns].copy()

for i,row in tqdm(product_df.iterrows()):
    columns=','.join(product_df.columns)
    values=','.join(['%s']*len(row))
    insert_query = f"""INSERT INTO {product_table} ({columns}) VALUES ({values})
                       ON CONFLICT (product_id) DO NOTHING;;"""
    cur.execute(insert_query,tuple(row))

location_columns=['postal_code','city','state','country','region']
location_df=df[location_columns].copy()

for i,row in tqdm(location_df.iterrows()):
    columns=','.join(location_df.columns)
    values=','.join(['%s']*len(row))
    insert_query = f"""INSERT INTO {location_table} ({columns}) VALUES ({values})
                       ON CONFLICT (postal_code) DO NOTHING;;"""
    cur.execute(insert_query,tuple(row))

conn.commit();
cur.close();
conn.close();

print("data loaded successfully")