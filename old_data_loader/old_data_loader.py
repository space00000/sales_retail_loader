#Modules and connection

from sqlalchemy import create_engine
from sqlite3 import connect
import psycopg2
import pandas as pd
import pyodbc
import os

# Open a database cursor

connection = psycopg2.connect(
    host='localhost',
    database='asus_db',
    user='postgres',
    password='GitsyLipsy6853',
    port='5432'
)

engine = create_engine('postgresql://postgres:GitsyLipsy6853@localhost:5432/asus_db')

sql_command = """ SELECT * FROM account"""

df_account = pd.read_sql(sql_command, connection)

df_account = df_account.rename(columns={'id':'account_id'})

# Database path

database_folder_path = 'C:/Users/GERARDITO/OneDrive - ASUS/Database/'

database_path = os.path.join(database_folder_path, 'Retail sales.accdb')

conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+database_path+';')
select_ventas = '''
        SELECT * FROM "Ventas - Consolidado";
        '''

df_ventas = pd.read_sql(select_ventas, conn)
df_ventas['Unidades'] = df_ventas['Unidades'].fillna(0)
df_ventas['Monto'] = df_ventas['Monto'].fillna(0)
df_ventas['StockUnits_app'] = df_ventas['StockUnits_app'].fillna(0)
df_ventas['StockMonto_app'] = df_ventas['StockMonto_app'].fillna(0)
index_names = df_ventas[ (df_ventas['Unidades'] == 0) &
                        (df_ventas['Monto'] == 0) &
                        (df_ventas['StockUnits_app'] == 0) &
                        (df_ventas['StockMonto_app'] == 0)].index
df_ventas.drop(index_names, inplace= True)
df_ventas['Fecha'] = pd.to_datetime(df_ventas['Fecha']).dt.date

df_ventas = df_ventas.rename(columns={'Fecha':'date_sales',
                                'Cliente':'account_name',
                                'Sucursal':'store_name',
                                'SKU':'sku',
                                'Unidades':'sell_out_units',
                                'Monto':'sell_out_value',
                                'StockUnits_app':'stock_units',
                                'StockMonto_app':'stock_value'})


df_ventas = df_ventas[["date_sales","account_name","store_name",
                        "sku","sell_out_units","sell_out_value",
                        "stock_units","stock_value"]]


df_ventas = pd.merge(df_ventas,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

conn.commit()
conn.close()

print(df_ventas.info())

psqlCursor = connection.cursor();

    # Name of the table to be deleted

tableName = "temp_old_sales";

    # Form the SQL statement - DROP TABLE

dropTableStmt = "DROP TABLE IF EXISTS %s;"%tableName;

    # Execute the drop table command

psqlCursor.execute(dropTableStmt);

    # Free the resources

connection.commit()

    #Create table

df_ventas.to_sql('temp_old_sales', engine)


    # Form the SQL statement - DROP TABLE

insert_sku = """INSERT INTO sku (sku, account_id) 
    SELECT DISTINCT t.sku, t.account_id 
    FROM temp_old_sales t
    WHERE NOT EXISTS (SELECT 1
                    FROM sku s
                    WHERE s.sku = t.sku
                    and s.account_id = t.account_id);"""

psqlCursor.execute(insert_sku);
connection.commit()

insert_store = """INSERT INTO store (store_original_name, account_id)
    SELECT DISTINCT t.store_name, t.account_id
    FROM temp_old_sales t
    WHERE NOT EXISTS (SELECT 1
                    FROM store s
                    WHERE s.store_original_name = t.store_name
                    and s.account_id = t.account_id);"""

psqlCursor.execute(insert_store);
connection.commit()

insert_sales = """INSERT INTO sales_retail (date_sales, store_id,sku_id, sell_out_units, sell_out_value, stock_units, stock_value)
        SELECT t.date_sales, st.id,sk.id, t.sell_out_units, t.sell_out_value, t.stock_units, t.stock_value
        FROM temp_old_sales t
        JOIN sku sk
        ON t.sku  = sk.sku and t.account_id = sk.account_id
        JOIN store st
        ON t.store_name = st.store_original_name and t.account_id = st.account_id"""

psqlCursor.execute(insert_sales);
connection.commit()

print('Datos subidos!')

    #End connection to database

psqlCursor.close();

connection.close();