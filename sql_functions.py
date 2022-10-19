#Modules and connection

import sales_retail_loader.clean_functions as clean_functions
import sales_disti_loader.clean_disti as clean_disti_functions
from sqlalchemy import create_engine
from sqlite3 import connect
import psycopg2

# Open a database cursor

connection = psycopg2.connect(
    host='localhost',
    database='asus_db',
    user='postgres',
    password='GitsyLipsy6853',
    port='5432'
)

engine = create_engine('postgresql://postgres:GitsyLipsy6853@localhost:5432/asus_db')

def upload_retail():

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


def upload_disti():

    psqlCursor = connection.cursor();

    # Name of the table to be deleted

    tableName = "temp_sales_disti";
    tableName2 = "temp_stock_disti";

    # Form the SQL statement - DROP TABLE

    dropTableStmt = "DROP TABLE IF EXISTS %s;"%tableName;
    dropTableStmt2 = "DROP TABLE IF EXISTS %s;"%tableName2;

    # Execute the drop table command

    psqlCursor.execute(dropTableStmt);
    psqlCursor.execute(dropTableStmt2);

    # Free the resources

    connection.commit()

    #Create table

    clean_disti_functions.df_disti_sales.to_sql('temp_sales_disti', engine)
    clean_disti_functions.df_disti_stock.to_sql('temp_stock_disti', engine)

    # Form the SQL statement - DROP TABLE

    insert_sku = """INSERT INTO sku_disti (sku, importer_id) 
    SELECT DISTINCT t.sku, t.importer_id 
    FROM temp_sales_disti t
    WHERE NOT EXISTS (SELECT 1
                    FROM sku_disti s
                    WHERE s.sku = t.sku
                    and s.importer_id = t.importer_id);"""

    psqlCursor.execute(insert_sku);
    connection.commit()

    insert_sku2 = """INSERT INTO sku_disti (sku, importer_id) 
    SELECT DISTINCT t.sku, t.importer_id 
    FROM temp_stock_disti t
    WHERE NOT EXISTS (SELECT 1
                    FROM sku_disti s
                    WHERE s.sku = t.sku
                    and s.importer_id = t.importer_id);"""

    psqlCursor.execute(insert_sku2);
    connection.commit()

    insert_buyer = """INSERT INTO buyer (buyer_code, importer_id)
    SELECT DISTINCT t.buyer_code, t.importer_id
    FROM temp_sales_disti t
    WHERE NOT EXISTS (SELECT 1
                    FROM buyer s
                    WHERE s.buyer_code = t.buyer_code
                    and s.importer_id = t.importer_id);"""

    psqlCursor.execute(insert_buyer);
    connection.commit()

    insert_sales = """INSERT INTO sales_distributor (date_sale, importer_id, sku_id, sell_out_units, sell_out_value, buyer_id)
        SELECT t.date, t.importer_id, sk.id, t.sell_out_units, t.sell_out_value, st.id
        FROM temp_sales_disti t
        JOIN sku_disti sk
        ON t.sku  = sk.sku and t.importer_id = sk.importer_id
        JOIN buyer st
        ON t.buyer_code = st.buyer_code and t.importer_id = st.importer_id;"""

    psqlCursor.execute(insert_sales);
    connection.commit()

    insert_stock = """INSERT INTO stock_distributor (date_stock, importer_id, sku_id, stock_units, stock_value)
        SELECT t.date, t.importer_id, sk.id, t.stock_units, t.stock_value
        FROM temp_stock_disti t
        JOIN sku_disti sk
        ON t.sku  = sk.sku and t.importer_id = sk.importer_id;"""

    psqlCursor.execute(insert_stock);
    connection.commit()

    print('Datos subidos!')

    #End connection to database

    psqlCursor.close();

    connection.close();
