import pandas as pd
import psycopg2
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()  # This loads variables from .env into the environment

# Set up tables
order_table='monthlysales.f_order'
customer_table='monthlysales.d_customer'
location_table='monthlysales.d_location'
product_table='monthlysales.d_product'

# Set up credentials
host = os.getenv('HOST')
port = os.getenv("PORT")
dbname = os.getenv("DBNAME")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

db_config={
    'host':host,
    'port':port,
    'dbname':dbname,
    'user':user,
    'password':password
}

# Read excel files
folder_path = "D://02ETL//monthly_sales_version2"
files = os.listdir(folder_path)
for file in tqdm(files):
    excel_file_path = folder_path + '//' + file
    #print("Working file: ", excel_file_path)

    # Read excel file as dataframe
    df = pd.read_excel(excel_file_path)
    # Rename columns of the dataframe
    columns_to_rename = ['row_id', 'order_id', 'order_date', 'ship_date', 'shipmode', 
                        'customer_id', 'customer_name', 'segment', 'country', 'city', 
                        'state', 'postal_code', 'region', 'product_id', 'category', 'sub_category', 
                        'product_name', 'sales', 'quantity', 'discount', 'profit']
    df.columns = columns_to_rename

    # Extract required columns
    required_column=['order_id','order_date','ship_date','shipmode','customer_id','postal_code','product_id','sales','quantity','discount','profit']
    dff=df[required_column].copy()

    # Change date data type for order date and ship date
    dff['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    dff['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')

    # Connect to database
    conn=psycopg2.connect(**db_config);
    cur=conn.cursor();

    # insert d_customers
    customer_columns=['customer_id','customer_name','segment']
    customer_df=df[customer_columns].copy()
    for i,row in customer_df.iterrows():
        columns=','.join(customer_df.columns)
        values=','.join(['%s']*len(row))
        insert_query = f"""INSERT INTO {customer_table} ({columns}) VALUES ({values})
                        ON CONFLICT (customer_id) DO NOTHING;;"""

        cur.execute(insert_query,tuple(row))


    # insert d_products
    product_columns=['product_id','product_name','sub_category','category']
    product_df=df[product_columns].copy()
    for i,row in product_df.iterrows():
        columns=','.join(product_df.columns)
        values=','.join(['%s']*len(row))
        insert_query = f"""INSERT INTO {product_table} ({columns}) VALUES ({values})
                        ON CONFLICT (product_id) DO NOTHING;;"""
        cur.execute(insert_query,tuple(row))

    # insert d_locations
    location_columns=['postal_code','city','state','country','region']
    location_df=df[location_columns].copy()
    for i,row in location_df.iterrows():
        columns=','.join(location_df.columns)
        values=','.join(['%s']*len(row))
        insert_query = f"""INSERT INTO {location_table} ({columns}) VALUES ({values})
                        ON CONFLICT (postal_code) DO NOTHING;;"""
        cur.execute(insert_query,tuple(row))

    # insert f_orders
    for i,row in dff.iterrows():
        columns=','.join(dff.columns)
        values=','.join(['%s']*len(row))
        insert_query=f'INSERT INTO {order_table}({columns}) VALUES({values})'
        cur.execute(insert_query,tuple(row))

    conn.commit();
    cur.close();
    conn.close();

    #print("Data is loaded successfully for ", excel_file_path)
    #print("\n")