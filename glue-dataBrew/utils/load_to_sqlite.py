import sqlite3
import pandas as pd
from sqlalchemy import *

db_path = f'C:\\sqlite\\'
data_path = f'D:\\dev\\data\\csv\\'

db_name = input("Please enter the name of the sqlite database without the .db extension: ")
file_name = input("Please enter the file name to be uploaded in the sqlite db: ")

def load_to_sql():
	#Read the CSV file using pandas
	df = pd.read_csv(f'{data_path}{file_name}.csv',sep=',')
	#Create the sql engine
	engine = create_engine(f'sqlite:///C:\\sqlite\\{db_name}.db',echo=True)
	#Migrating data from pandas to sql db
	df.to_sql(f'{db_name}',con=engine,if_exists='replace')

#Executing function with default parameters
load_to_sql()
