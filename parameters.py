import psycopg2
from sqlalchemy import create_engine
from sqlite3 import connect

# Connection to database

connection = psycopg2.connect(
    host='localhost',
    database='asus_db',
    user='postgres',
    password='GitsyLipsy6853',
    port='5432'
)


engine = create_engine('postgresql://postgres:GitsyLipsy6853@localhost:5432/asus_db')
