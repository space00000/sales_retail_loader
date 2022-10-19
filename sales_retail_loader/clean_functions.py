import pandas as pd
import datetime as datetime
import numpy as np
from numpy import inf
from datetime import date
from datetime import timedelta
import psycopg2
import os
import re
import psycopg2

# Connection to database

connection = psycopg2.connect(
    host='localhost',
    database='asus_db',
    user='postgres',
    password='GitsyLipsy6853',
    port='5432'
)


#Global variables

df = pd.DataFrame()

#Funciones

def iva(x):
                return 1.19*x

#Archivos excel

retail_data_path = 'C:/Users/GERARDITO/OneDrive - ASUS/Python/retail_data_cleaner/retail_data/'

database_path_abc = os.path.join(retail_data_path, 'ABCDIN.xlsx')

database_path_eshop = os.path.join(retail_data_path, 'E-shop.xlsx')

database_path_falabella = os.path.join(retail_data_path, 'FALABELLA.xlsx')

database_path_hites = os.path.join(retail_data_path, 'HITES.xlsx')

database_path_lapolarstock = os.path.join(retail_data_path, 'LA POLAR STOCK.xlsx')

database_path_lapolar = os.path.join(retail_data_path, 'LA POLAR.xlsx')

database_path_paris = os.path.join(retail_data_path, 'PARIS.xlsx')

database_path_pcfactory = os.path.join(retail_data_path, 'PCFACTORY.xlsx')

database_path_ripleystock = os.path.join(retail_data_path, 'RIPLEY STOCK.csv')

database_path_ripley = os.path.join(retail_data_path, 'RIPLEY VENTA.csv')

database_path_pcfactorystock = os.path.join(retail_data_path, 'STOCK PCFACTORY.xlsx')

database_path_walmart = os.path.join(retail_data_path, 'WALMART.xlsx')

#SQL Account table to look for account id

sql_command = """ SELECT * FROM account"""

df_account = pd.read_sql(sql_command, connection)

df_account = df_account.rename(columns={'id':'account_id'})

#Clean functions

def clean_abc():

        abc_sales_data = pd.read_excel(database_path_abc, sheet_name = 'VENTA')

        df_abc_sales = pd.DataFrame(abc_sales_data)

        df_abc_sales = df_abc_sales.rename(columns={'Unnamed: 1':'store_name', 'Día':'date_sales',
                                                'Venta en Unidades':'sell_out_units', 'Monto Producto Neto':'sell_out_value',
                                                'Producto' : 'sku'})

        df_abc_sales['account_name'] = "Abc"

        df_abc_sales['sell_out_value'] = df_abc_sales['sell_out_value'].apply(iva)

        df_abc_sales['stock_units'] = 0

        df_abc_sales['stock_value'] = 0

        df_abc_sales['date_sales'] = df_abc_sales['date_sales'].dt.date

        df_abc_sales['sku'] = df_abc_sales['sku'].apply(lambda x: str(x))

        df_abc_sales = df_abc_sales[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value","stock_units","stock_value"]]

        #ABC archivo stock
        
        abc_stock_data = pd.read_excel(database_path_abc, sheet_name = 'STOCK TIENDA')

        df_abc_stock = pd.DataFrame(abc_stock_data)

        df_abc_stock['account_name'] = "Abc"

        df_abc_stock = df_abc_stock.rename(columns={'Stock Disponible de Tienda':'stock_units', 'date':'date_sales', 
                                                "Descripcion del Local":'store_name','SKU':'sku'})

        df_abc_stock = df_abc_stock[df_abc_stock.stock_units != 0]

        df_abc_stock['stock_value'] = df_abc_stock['stock_units'] * df_abc_stock['Costo promedio']

        df_abc_stock['sell_out_units'] = 0

        df_abc_stock['sell_out_value'] = 0

        df_abc_stock['sku'] = df_abc_stock['sku'].apply(lambda x: str(x))

        # Get sunday

        day_s = df_abc_sales['date_sales'].iloc[0].strftime("%d-%m-%Y")
        dt = datetime.datetime.strptime(day_s, '%d-%m-%Y')
        start = dt - timedelta(days=dt.weekday())
        end_week = start + timedelta(days=6)

        df_abc_stock['date_sales'] = end_week

        df_abc_stock['date_sales'] = df_abc_stock['date_sales'].dt.date

        # Select columns

        df_abc_stock = df_abc_stock[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value","stock_units","stock_value"]]

        # Unir account id

        global df

        df = pd.concat([df_abc_sales, df_abc_stock])

        df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

        print('Limpieza realizada!')

        connection.close();

def clean_eshop():
    #Eshop ventas

    eshop = pd.read_excel(database_path_eshop, sheet_name = 'Detalle')

    df0 = pd.DataFrame(eshop)

    df0['account_name'] = "E-shop"

    df0 = df0.rename(columns={'DT':'date_sales', 'CUSTOMER':'store_name', 'QTY':'sell_out_units', 'SALE':'sell_out_value', 'SKU':'sku'})

    df0['date_sales'] = pd.to_datetime(df0['date_sales']).dt.date
    df0['sell_out_value'] = df0.sell_out_value.apply(lambda x: x*1.19*950)
    df0['stock_units'] = 0
    df0['stock_value'] = 0
    df0['sku'] = df0['sku'].apply(lambda x: str(x))

    #choose columns that I need to use that will be my new df

    df0 = df0[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value", 'stock_units', 'stock_value']]

    #Eshop stock

    #Definir database

    eshop = pd.read_excel(database_path_eshop, sheet_name = 'Stock')

    df1 = pd.DataFrame(eshop)

    df1['account_name'] = "E-shop"

    day_s = df0['date_sales'].iloc[0].strftime("%d-%m-%Y")
    dt = datetime.datetime.strptime(day_s, '%d-%m-%Y')
    start = dt - timedelta(days=dt.weekday())
    end_week = start + timedelta(days=6)

    df1['date_sales'] = end_week

    df1 = df1.rename(columns={'DT':'date_sales', 'SEGMENT':'store_name', 'ATP':'stock_units', 'Precio':'stock_amount1',
                         'Material':'sku','Dscto ACOP':'stock_amount2'})

    df1['store_name'] = 'ANOVO ASUS'
    df1['stock_amount'] = df1['stock_amount1'] - df1['stock_amount2']
    df1['sell_out_units'] = 0
    df1['sell_out_value'] = 0
    df1 = df1[df1.stock_units != 0]
    df1['stock_value'] = df1['stock_units'] * df1['stock_amount'] * 820 * 1.19
    df1['date_sales'] = pd.to_datetime(df1['date_sales']).dt.date
    df1['sku'] = df1['sku'].apply(lambda x: str(x))


    #choose columns that I need to use that will be my new df

    df1 = df1[["date_sales","account_name","store_name","sku", 'sell_out_units', 'sell_out_value', "stock_units","stock_value"]]

    #Concat e-shop

    global df

    df = pd.concat([df0 , df1])

    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    print('Limpieza realizada!')

    connection.close();

def clean_falabella():

    #Definir database

    f_data = pd.read_excel(database_path_falabella)

    df1 = pd.DataFrame(f_data)

    #Definir fechas calculadas

    #Definir año
    this_year = datetime.datetime.today().strftime("%Y")

    #Definir primer día de ventas, a partir del nombre de columna
    colname1 = df1.columns[10]
    separator = '_'
    pattern = ".*" + separator
    colname1 = re.sub(pattern, '', colname1)
    full_date = colname1 + "-" + this_year
    date1 = datetime.datetime.strptime(full_date, "%d-%m-%Y")
    date2 = date1 + timedelta(days=1)
    date3 = date2 + timedelta(days=1)
    date4 = date3 + timedelta(days=1)
    date5 = date4 + timedelta(days=1)
    date6 = date5 + timedelta(days=1)
    date7 = date6 + timedelta(days=1)

    #Definir ventas por día

    df1['sell_out_units'] = df1.iloc[:,10]

    df1['sales_units2'] = df1.iloc[:,11]

    df1['sales_units3'] = df1.iloc[:,12]

    df1['sales_units4'] = df1.iloc[:,13]

    df1['sales_units5'] = df1.iloc[:,14]

    df1['sales_units6'] = df1.iloc[:,15]

    df1['sales_units7'] = df1.iloc[:,16]

    #Customer name

    df1['account_name'] = "Falabella"

    #Días definidos
    df1['date_sales'] = date1

    df1['date2'] = date2

    df1['date3'] = date3

    df1['date4'] = date4

    df1['date5'] = date5

    df1['date6'] = date6

    df1['date7'] = date7

    #Aca empiezo a construir el DF una vez que las fechas están definidas

    df1['unit_value'] = df1['VENTA_PESOS'] / df1['VENTA_UNIDADES']

    df1['unit_value'] = df1['unit_value'].fillna(0)

    df1['stock_units'] = 0

    df1['stock_value'] = 0

    df1['stock_units1'] = df1['STOCK']

    df1['stock_amount1'] = 0

    df1 = df1.rename(columns={'MODELO':'modelo', 'LOCAL':'store_name', 'DESCRIPCION_LARGA':'desc', 'SKU':'sku'})

    df1['sell_out_value'] = df1['sell_out_units'] * df1['unit_value']

    df1['sales_amount2'] = df1['sales_units2'] * df1['unit_value']

    df1['sales_amount3'] = df1['sales_units3'] * df1['unit_value']

    df1['sales_amount4'] = df1['sales_units4'] * df1['unit_value']

    df1['sales_amount5'] = df1['sales_units5'] * df1['unit_value']

    df1['sales_amount6'] = df1['sales_units6'] * df1['unit_value']

    df1['sales_amount7'] = df1['sales_units7'] * df1['unit_value']

    df2 = df1[df1.sell_out_units != 0]
    df3 = df1[df1.sales_units2 != 0]
    df4 = df1[df1.sales_units3 != 0]
    df5 = df1[df1.sales_units4 != 0]
    df6 = df1[df1.sales_units5 != 0]
    df7 = df1[df1.sales_units6 != 0]
    df8 = df1[df1.sales_units7 != 0]

    #choose columns that I need to use that will be my new df

    df2 = df1[["account_name","date_sales","sku","store_name","sell_out_units","sell_out_value","stock_units","stock_value"]]

    df3 = df1[["account_name","date2","sku","store_name","sales_units2","sales_amount2","stock_units","stock_value"]]

    df4 = df1[["account_name","date3","sku","store_name","sales_units3","sales_amount3","stock_units","stock_value"]]

    df5 = df1[["account_name","date4","sku","store_name","sales_units4","sales_amount4","stock_units","stock_value"]]

    df6 = df1[["account_name","date5","sku","store_name","sales_units5","sales_amount5","stock_units","stock_value"]]

    df7 = df1[["account_name","date6","sku","store_name","sales_units6","sales_amount6","stock_units","stock_value"]]

    df8 = df1[["account_name","date7","sku","store_name","sales_units7","sales_amount7","stock_units1","stock_amount1"]]

    global df

    df = pd.concat([df2 ,
                 df3.rename(columns={'sales_units2':'sell_out_units', 'sales_amount2':'sell_out_value', 'date2':'date_sales'}),
                 df4.rename(columns={'sales_units3':'sell_out_units', 'sales_amount3':'sell_out_value', 'date3':'date_sales'}),
                df5.rename(columns={'sales_units4':'sell_out_units', 'sales_amount4':'sell_out_value', 'date4':'date_sales'}),
                df6.rename(columns={'sales_units5':'sell_out_units', 'sales_amount5':'sell_out_value', 'date5':'date_sales'}),
                df7.rename(columns={'sales_units6':'sell_out_units', 'sales_amount6':'sell_out_value', 'date6':'date_sales'}),
                df8.rename(columns={'sales_units7':'sell_out_units', 'sales_amount7':'sell_out_value', 'date7':'date_sales', 
                                    'stock_units1':'stock_units', 'stock_amount1':'stock_value'})], ignore_index=True)

    df.replace([np.inf, -np.inf], 0, inplace=True)

    df = df[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value","stock_units","stock_value"]]
    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')
    index_names = df[ (df['sell_out_units'] == 0) & (df['stock_units'] == 0)].index
    df.drop(index_names, inplace= True)
    df['sell_out_value'] = df['sell_out_value'].fillna(0)

    print('Limpieza realizada!')

    connection.close();

def clean_hites():
    #Hites archivo ventas y stock

    #Definir database

    hites_data = pd.read_excel(database_path_hites, sheet_name = 'BD')

    df_hites = pd.DataFrame(hites_data)

    df_hites['account_name'] = "Hites"

    df_hites = df_hites.rename(columns={'U VENTA TOTAL':'sell_out_units',
                                        '$ VENTA TOTAL':'sell_out_value',
                                        'U STOCK DISPONIBLE':'stock_units',
                                        '$ STOCK CONTABLE VAL COSTO':'stock_value',
                                        'LOCAL':'store_name',
                                        'PRODUCTO':'sku'})

    df_hites['sell_out_units'] = df_hites.sell_out_units.replace(regex=['-'], value=0)

    df_hites['sell_out_value'] = df_hites.sell_out_value.replace(regex=['-'], value=0)

    df_hites['stock_units'] = df_hites.stock_units.replace(regex=['-'], value=0)

    df_hites['stock_value'] = df_hites.stock_value.replace(regex=['-'], value=0)

    df_hites['date_sales'] = pd.to_datetime(df_hites['DIA']).dt.date

    min_date = df_hites['date_sales'].min()

    max_date = min_date + timedelta(days=6)

    df_hites['stock_units'] = df_hites.stock_units.where(df_hites.date_sales == max_date, 0)

    df_hites['stock_value'] = df_hites.stock_value.where(df_hites.date_sales == max_date, 0)

    df_hites = df_hites[df_hites.PROVEEDOR == 'INGRAM MICRO CHILE S.A.']

    #choose columns that I need to use that will be my new df

    df_hites = df_hites[["date_sales","account_name","store_name",
                        "sku","sell_out_units","sell_out_value",
                        "stock_units","stock_value"]]

    index_names = df_hites[ (df_hites['sell_out_units'] == 0) & (df_hites['stock_units'] == 0)].index
    df_hites.drop(index_names, inplace= True)

    global df

    df = pd.merge(df_hites,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    df['sku'] = df['sku'].apply(lambda x: str(x))

    print('Limpieza realizada!')

    connection.close();

def clean_lapolar():
    #La Polar archivo ventas y stock

    #Definir database

    lp_data = pd.read_excel(database_path_lapolar, sheet_name = 'Hoja1')

    df_lp = pd.DataFrame(lp_data)

    df_lp['account_name'] = "La Polar"

    df_lp = df_lp.rename(columns={'Bodega Asociada':'store_name',
                                'Venta Uni P1':'sell_out_units',
                                'Venta Neta P1':'sell_out_value',
                                df_lp.columns[17]:'stock_units',
                                df_lp.columns[18]:'stock_value',
                                'Fecha':'date_sales'})

    df_lp['store'] = df_lp['store_name'].str.replace('.','',regex=False)

    df_lp['sku'] = df_lp['Plu'].str[:8]

    df_lp = df_lp[df_lp.date_sales.str.contains('2022')]

    def double(x):
        return 1000*1.19*x

    df_lp['sell_out_value'] = df_lp['sell_out_value'].apply(double)

    df_lp['stock_units'] = 0

    df_lp['stock_value'] = 0

    #choose columns that I need to use that will be my new df

    df_lp['date_sales'] = pd.to_datetime(df_lp['date_sales'], format='%d-%m-%Y')

    min_date = df_lp['date_sales'].min()
    max_date = min_date + timedelta(days=6)

    df_lp = df_lp[["date_sales","account_name","store_name",
                        "sku","sell_out_units","sell_out_value",
                        "stock_units","stock_value"]]

    #La Polar archivo stock

    lps = pd.read_excel(database_path_lapolarstock)

    df_s = pd.DataFrame(lps)

    df_s['account_name'] = "La Polar"

    df_s = df_s.rename(columns={'Tienda':'store_name', 'PLU':'sku', 'Stock Disp Tot':'stock_units'})

    df_s['store_name'] = df_s['store_name'].str.replace('.','',regex=False)

    df_s['sell_out_units'] = 0
    df_s['sell_out_value'] = 0
    df_s['stock_value'] = 0

    df_s['date_sales'] = max_date

    df_s = df_s[df_s.stock_units != 0]

    #choose columns that I need to use that will be my new df

    df_s = df_s[["date_sales","account_name","store_name",
                        "sku","sell_out_units","sell_out_value",
                        "stock_units","stock_value"]]

    global df

    df = pd.concat([df_lp, df_s])

    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    df['sku'] = df['sku'].apply(lambda x: str(x))

    df['date_sales'] = df['date_sales'].dt.date

    print('Limpieza realizada!')

    connection.close();

def clean_paris():

    #Paris archivo ventas

    ps_data = pd.read_excel(database_path_paris, sheet_name='Base Vta Diaria')

    df_ps = pd.DataFrame(ps_data)

    df_ps['account_name'] = "Paris"

    df_ps = df_ps.rename(columns={'Dia Comercial':'date_sales',
                                df_ps.columns[13]:'sell_out_value',
                                df_ps.columns[14]:'sell_out_units',
                                'Item':'sku',
                                df_ps.columns[12]:'store_name'})

    df_ps['stock_units'] = 0

    df_ps['stock_value'] = 0

    min_date = df_ps['date_sales'].min()
    max_date = min_date + timedelta(days=6)

    #choose columns that I need to use that will be my new df

    df_ps = df_ps[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value","stock_units","stock_value"]]

    #Paris archivo stock

    pss_data = pd.read_excel(database_path_paris, sheet_name='Base Share')

    df_pss = pd.DataFrame(pss_data)

    df_pss['account_name'] = "Paris"

    df_pss['date_sales'] = max_date

    df_pss = df_pss.rename(columns={'stock unidades':'stock_units',
                                    'stock a costo':'stock_value',
                                    'Desc Tienda':'store_name',
                                    'Item':'sku'})

    df_pss['stock_units'] = df_pss['stock_units'].fillna(0)

    df_pss = df_pss[df_pss.stock_units != 0]

    df_pss['sell_out_units'] = 0

    df_pss['sell_out_value'] = 0

    #choose columns that I need to use that will be my new df

    df_pss = df_pss[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value","stock_units","stock_value"]]

    #Exportar excel

    global df

    df = pd.concat([df_ps, df_pss])

    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    df['sku'] = df['sku'].apply(lambda x: str(x))

    df['date_sales'] = df['date_sales'].dt.date

    print('Limpieza realizada!')

    connection.close();

def clean_pcfactory():

    #PCF archivo ventas

    pcf_data = pd.read_excel(database_path_pcfactory)

    df_pcf = pd.DataFrame(pcf_data)

    df_pcf = df_pcf.rename(columns={'FECHA':'date_sales',
                                    'SUCURSAL':'store_name',
                                    'ID PCFACTORY':'sku',
                                    'CANTIDAD':'sell_out_units'})

    df_pcf['account_name'] = "PcFactory"

    df_pcf['stock_units'] = 0

    df_pcf['stock_value'] = 0

    df_pcf['sell_out_value'] = df_pcf['NETO'].apply(iva)

    min_date = df_pcf['date_sales'].min()
    max_date = min_date + timedelta(days=6)

    #choose columns that I need to use that will be my new df

    df_pcf = df_pcf[["date_sales","account_name",
                    "store_name","sku","sell_out_units",
                    "sell_out_value","stock_units","stock_value"]]

    #PCFactory stock file

    dfpc = pd.read_excel(database_path_pcfactorystock)

    df_stock = pd.DataFrame(dfpc)

    df_stock = df_stock.rename(columns={'Sucursal':'store_name',
                                'ID PCFACTORY':'sku',
                                'Stock':'stock_units',
                                'Inventario':'stock_value'})

    #create columns date and customer for PcFactory stock

    df_stock['date_sales'] = max_date
    df_stock['account_name'] = "PcFactory"
    df_stock['store_name'] = df_stock.store_name.replace(regex=['B_'], value='')
    df_stock['store_name'] = df_stock.store_name.replace(regex=['_'], value=' ')

    df_stock['sell_out_units'] = 0

    df_stock['sell_out_value'] = 0

    df_stock = df_stock[["date_sales","account_name",
                        "store_name","sku","sell_out_units",
                        "sell_out_value","stock_units","stock_value"]]

    global df

    df = pd.concat([df_pcf, df_stock])

    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    df['sku'] = df['sku'].apply(lambda x: str(x))

    df['date_sales'] = df['date_sales'].dt.date

    print('Limpieza realizada!')

    connection.close();

def clean_ripley():
    #Ripley archivo ventas

    rip_sales_data = pd.read_csv(database_path_ripley, sep=';', index_col = False)

    df_rs = pd.DataFrame(rip_sales_data)

    df_rs = df_rs.rename(columns={'Fecha':'date_sales',
                                    'Sucursal':'store_name',
                                    'Cod Modelo':'sku',
                                    'Unidades':'sell_out_units',
                                    'Vta Rtl':'sell_out_value'})

    df_rs['date_sales'] = pd.to_datetime(df_rs['date_sales']).dt.date

    min_date = df_rs['date_sales'].min()
    max_date = min_date + timedelta(days=6)

    df_rs['account_name'] = "Ripley"

    df_rs['stock_units'] = 0

    df_rs['stock_value'] = 0

    df_rs['sell_out_value'] = df_rs['sell_out_value'].apply(iva)

    df_rs = df_rs[df_rs.sku != 'REBATES']

    #choose columns that I need to use that will be my new df

    df_rs = df_rs[["date_sales","account_name",
                    "store_name","sku","sell_out_units",
                    "sell_out_value","stock_units","stock_value"]]

    #Ripley archivo stock

    rip_stock_data = pd.read_csv(database_path_ripleystock , sep=';', index_col = False)

    df_rss = pd.DataFrame(rip_stock_data)

    df_rss = df_rss.rename(columns={'Sucursal':'store_name',
                                    'Cod Mod':'sku',
                                    'und fis':'stock_units',
                                    'cot fis':'stock_value'})

    df_rss['sell_out_units'] = 0

    df_rss['sell_out_value'] = 0

    df_rss['account_name'] = "Ripley"

    df_rss['date_sales'] = max_date

    #choose columns that I need to use that will be my new df

    df_rss = df_rss[["date_sales","account_name",
                    "store_name","sku","sell_out_units",
                    "sell_out_value","stock_units","stock_value"]]

    global df

    df = pd.concat([df_rs, df_rss])

    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    df['sku'] = df['sku'].apply(lambda x: str(x))

    index_names = df[ (df['sell_out_units'] == 0) & (df['stock_units'] == 0)].index
    df.drop(index_names, inplace= True)

    print('Limpieza realizada!')

    connection.close();

def clean_walmart():
    #Walmart archivo ventas y stock

    #Definir database

    w_data = pd.read_excel(database_path_walmart)

    df_test = pd.DataFrame(w_data)

    df_test = df_test.rename(columns={'Cnt POS':'sales_units', 'Diario (Sólo POS)':'date'})

    df_test = df_test[df_test.sales_units != 0]

    df_test['date'] = pd.to_datetime(df_test['date']).dt.date

    min_date = df_test['date'].min()
    max_date = min_date + timedelta(days=6)

    dfw = pd.DataFrame(w_data)

    dfw = dfw.rename(columns={'Diario (Sólo POS)':'date',
                                'Núm Artículo':'SKU',
                                'Nombre de Tienda':'store',
                                'Cnt POS':'sales_units',
                                'Cantidad Actual en Existentes de la tienda':'stock_tienda',
                                'Venta POS':'sales_amount',
                                'Costo Unidad':'stock_amount',
                                'Ctd En-Existencia CD':'stock_online'})

    dfw['stock_amount'] = dfw.stock_amount.apply(lambda x: x*100)

    dfw['customer'] = "Walmart"

    dfw['date1'] = max_date

    dfw['sales_amount'] = dfw.sales_amount.apply(lambda x: x*100*1.19)

    amount_online_store = lambda row: row.sales_amount / 1.19 \
    if row.store == 'EL PEñON, ECOMERCE 6020' or row.store == 'EL PEñON ECOMERCE 6020' \
    else row.sales_amount
    
    dfw['sales_amount'] = dfw.apply(amount_online_store, axis = 1)

    dfw['stock_amount1'] = dfw['stock_amount'] * dfw['stock_tienda']

    dfw['stock_amount2'] = dfw['stock_amount'] * dfw['stock_online']

    dfw['store_online'] = dfw['store']

    dfw['store_online'] = 'Web'

    dfw['store_fisico'] = dfw['store']

    #Definir ventas

    dfw1 = dfw

    dfw1['stock_units'] = 0

    dfw1['stock_amount'] = 0

    dfw1['date'] = dfw1.date.replace(regex=['/'], value='-')

    dfw1['date'] = pd.to_datetime(dfw1['date']).dt.date

    dfw1 = dfw1[dfw1.sales_units != 0]

    #Definir stock tienda

    dfw2 = dfw

    dfw2['fake_units'] = 0

    dfw2['fake_amount'] = 0

    dfw2 = dfw2[dfw2.store_fisico != 'EL PEñON ECOMERCE 6020']

    dfw2 = dfw2[dfw2.store_fisico != 'ECOMMERCE GM']

    dfw2 = dfw2[dfw2.store_fisico != 'EL PEñON, ECOMERCE 6020']

    dfw2 = dfw2[dfw2.stock_tienda != 0]

    #Definir stock online

    dfw3 = dfw

    dfw3['fake_units'] = 0

    dfw3['fake_amount'] = 0

    dfw3 = dfw3[dfw3.stock_online != 0]
                            
    #choose columns that I need to use that will be my new df

    dfw1 = dfw1[['date','customer','SKU', 'store','sales_units','sales_amount','stock_units','stock_amount']]

    dfw2 = dfw2[['date1','customer','SKU', 'store_fisico','fake_units','fake_amount','stock_tienda','stock_amount1']]

    dfw3 = dfw3[['date1','customer','SKU', 'store_online','fake_units','fake_amount','stock_online','stock_amount2']]

    global df

    df = pd.concat([dfw1 ,
                    dfw2.rename(columns={'date1':'date', 'store_fisico':'store','stock_tienda':'stock_units', 'stock_amount1':'stock_amount', 'fake_units':'sales_units', 'fake_amount':'sales_amount'}),
                    dfw3.rename(columns={'date1':'date', 'store_online':'store', 'stock_online':'stock_units', 'stock_amount2':'stock_amount', 'fake_units':'sales_units', 'fake_amount':'sales_amount'})],ignore_index=True)
                  
    df = df.rename(columns={'date':'date_sales',
                                     'store':'store_name',
                                     'SKU':'sku',
                                     'stock_amount':'stock_value',
                                     'sales_units':'sell_out_units',
                                     'sales_amount':'sell_out_value',
                                     'customer':'account_name'})

    df = df[["date_sales","account_name","store_name","sku","sell_out_units","sell_out_value","stock_units","stock_value"]]

    df = pd.merge(df,df_account[['account_name','account_id']],left_on='account_name', right_on='account_name', how='inner')

    df['sku'] = df['sku'].apply(lambda x: str(x))

    print('Limpieza realizada!')

    connection.close();

clean_abc()
print(df)