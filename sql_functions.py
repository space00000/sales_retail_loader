#Modules and connection

import clean_functions
from clean_functions import *
from sqlalchemy import create_engine
from sqlite3 import connect

# Open a database cursor

connection = psycopg2.connect(
    host='localhost',
    database='asus_db',
    user='postgres',
    password='GitsyLipsy6853',
    port='5432'
)

engine = create_engine('postgresql://postgres:GitsyLipsy6853@localhost:5432/asus_db')

def upload():

    psqlCursor = connection.cursor();

    # Name of the table to be deleted

    tableName = "temp_sales";

    # Form the SQL statement - DROP TABLE

    dropTableStmt = "DROP TABLE IF EXISTS %s;"%tableName;

    # Execute the drop table command

    psqlCursor.execute(dropTableStmt);

    # Free the resources

    connection.commit()

    #Create table

    clean_functions.df.to_sql('temp_sales', engine)

    # Form the SQL statement - DROP TABLE

    insert_sku = """INSERT INTO sku (sku, account_id) 
    SELECT DISTINCT t.sku, t.account_id 
    FROM temp_sales t
    WHERE NOT EXISTS (SELECT 1
                    FROM sku s
                    WHERE s.sku = t.sku
                    and s.account_id = t.account_id);"""

    psqlCursor.execute(insert_sku);
    connection.commit()

    insert_store = """INSERT INTO store (store_original_name, account_id)
    SELECT DISTINCT t.store_name, t.account_id
    FROM temp_sales t
    WHERE NOT EXISTS (SELECT 1
                    FROM store s
                    WHERE s.store_original_name = t.store_name
                    and s.account_id = t.account_id);"""

    psqlCursor.execute(insert_store);
    connection.commit()

    insert_sales = """INSERT INTO sales_retail (date_sales, store_id,sku_id, sell_out_units, sell_out_value, stock_units, stock_value)
        SELECT t.date_sales, st.id,sk.id, t.sell_out_units, t.sell_out_value, t.stock_units, t.stock_value
        FROM temp_sales t
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