import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import os
import psycopg2

#Carpetas donde se encuentran los archivos

retail_data_path = 'C:/Users/GERARDITO/OneDrive - ASUS/Python/distributor_data_cleaner/Input/'

input_ingram = os.path.join(retail_data_path, 'INGRAM.xlsx')

input_ingram_stock = os.path.join(retail_data_path, 'INGRAM STOCK.xlsx')

input_intcomex = os.path.join(retail_data_path, 'INTCOMEX.xlsx')

input_intcomex_stock = os.path.join(retail_data_path, 'INTCOMEX STOCK.xlsx')

input_nexsys = os.path.join(retail_data_path, 'NEXSYS.xlsx')

# Variable global para obtener el ultimo dataframe limpiado

df_disti_sales = pd.DataFrame()

df_disti_stock = pd.DataFrame()

# Conexion a database para obtener id

connection = psycopg2.connect(
    host='localhost',
    database='asus_db',
    user='postgres',
    password='GitsyLipsy6853',
    port='5432'
)

sql_command = """ SELECT * FROM importer"""

df_importer = pd.read_sql(sql_command, connection)

df_importer = df_importer.rename(columns={'id':'importer_id'})


def clean_intcomex():

    #Sales formatting

    intcomex_sales = pd.read_excel(input_intcomex)

    df = pd.DataFrame(intcomex_sales)

    df = df.rename(columns={'Trans Date':'date',
                                 'Ship Qty':'sell_out_units',
                                 'Unit Initial Cost US':'sales_cost',
                                 'CustomerID':'buyer_code',
                                 'SKU':'sku'})

    df['importer_name'] = 'Intcomex'

    df['sell_out_value'] = df['sell_out_units'] * df['sales_cost']

    df = df[df['sku'].notna()]

    df = df[['date','importer_name','sku','sell_out_units','sell_out_value','buyer_code']]

    #Stock

    intcomex_stock = pd.read_excel(input_intcomex_stock, header = 2)

    df1 = pd.DataFrame(intcomex_stock)

    #Obtener fecha del domingo

    day_s = df['date'].iloc[0].strftime("%d-%m-%Y")
    dt1 = dt.strptime(day_s, '%d-%m-%Y')
    start = dt1 - timedelta(days=dt1.weekday())
    end_week = start + timedelta(days=6)

    df1['date'] = end_week

    df1['importer_name'] = 'Intcomex'

    df1 = df1.rename(columns={'OH':'stock_units',
                                 'InitialCostUS':'stock_cost',
                                 'SKU':'sku'})

    df1['stock_value'] = df1['stock_units'] * df1['stock_cost']

    df1 = df1[['date','importer_name','sku','stock_units','stock_value']]

    index_names = df1[ (df1['stock_units'] == 0) & (df1['stock_value'] == 0)].index
    df1.drop(index_names, inplace= True)

    df['date'] = pd.to_datetime(df['date']).dt.date
    df1['date'] = pd.to_datetime(df1['date']).dt.date

    global df_disti_stock

    global df_disti_sales

    df = pd.merge(df,df_importer[['importer_name','importer_id']],left_on='importer_name', right_on='importer_name', how='inner')

    df1 = pd.merge(df1,df_importer[['importer_name','importer_id']],left_on='importer_name', right_on='importer_name', how='inner')

    df_disti_sales = df

    df_disti_stock = df1