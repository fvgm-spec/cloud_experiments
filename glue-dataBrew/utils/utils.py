import requests
import datetime as dt
from datetime import date, datetime, timedelta
import pandas as pd
import json
import traceback
from configparser import ConfigParser
import time

#Other libraries
import psycopg2
import sqlalchemy as db
from sqlalchemy import create_engine

#getting secrets from config.ini file
config = ConfigParser()
config.read('./cfg/config.ini')
#url to the endpoint choose the one from the config.ini file corresponding to the raw data to load in postgres table
url = config.get('instance', 'url_people')
webhook_url = config.get('instance', 'slack_url')
token = config.get('auth', 'float_token')
host = config.get('hosts', 'float_host')
pwd = config.get('passwords', 'float_pwd')
client = config.get('auth', 'slack_token')

#defining variables
out_path = './outputs/'
passw = pwd
hostName = host
port = 5432
dbName = 'postgres'

#sets a token for API connection
auth_token = 'Bearer {}'.format(token)
#Set type of returned data
accept_format = 'application/json'
#setting headers
headers= {'Accept': accept_format, 'Authorization': auth_token}
#sets connection to database
conn = f"postgresql+psycopg2://postgres:{pwd}@{host}:{port}/{dbName}"
engine = db.create_engine(conn,executemany_mode='values', executemany_values_page_size=50000)

#Setting datetime variables
today = dt.datetime.now()                #Current day of the month
delta_minus_month = today - timedelta(days=30) #1 month back from the current day
delta_plus_month = today + timedelta(days=31) #1 month up from the current day
first = datetime.today().replace(day=1) #first day of the month
#Datetime variables to integer
first = int(first.strftime("%d")) #first day integer  //1//
start_month = int(delta_minus_month.strftime("%m")) #1 month back
current_month = int(today.strftime("%m")) #current month
end_month = int(delta_plus_month.strftime("%m")) #1 month forward
start_year = int(delta_minus_month.strftime("%Y")) #1 year back
current_year = int(today.strftime("%Y")) #current year
end_year = int(delta_plus_month.strftime("%Y")) #1 year forward
year_month = str(f"{current_year}{current_month}")

##Setting functions to work on scripts 
def log(message,log_filename):
    """
    Parameters
    ----------
    message: string to be written in the log
    """
    # set timestamp format
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    # get current timestamp
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(f"./logs/logfile_{log_filename}.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

#defines a function to get the number for pages from API endpoint 
def getting_endpoint_data(endpoint):
    """
    Parameters
    ----------
    endpoint: name of the endpoint from the config.ini file
    """
    get_endpoint_data = requests.get(endpoint+'?per-page=200&page=0', headers=headers)
    pages = get_endpoint_data.headers['X-Pagination-Page-Count']
    
    return pages

#defines a function to get the number for pages from API endpoint 
def getting_endpoint_data_logged(endpoint,start_date,end_date):
    """
    Parameters
    ----------
    endpoint: name of the endpoint from the config.ini file
    """
    get_endpoint_data = requests.get(endpoint+f'?per-page=200&page=0&start_date={start_date}&end_date={end_date}', headers=headers)
    pages = get_endpoint_data.headers['X-Pagination-Page-Count']
    
    return pages

#defines a function to extract data from API endpoint
def extracting_data_from_api_general(endpoint,pages):
    """
    Parameters
    ----------
    endpoint: name of the endpoint from the config.ini file
    pages: number of pages get from previous function
    """
    ##Extracting data from the planned_tasks API
    #Creates a new dataframe to store the tasks

    df = pd.DataFrame() 
    #Starts looping over the API endpoint
    i=1
    while i<=int(pages):
        print(f"Page {i} from {pages}")
        #Iterates over the tasks endpoint extracting data for the required time range    
        r = requests.get(endpoint+f'?per-page=200&page={i}', headers=headers)
        table = pd.DataFrame(r.json(),index=list(range(len(r.json()))))
        df = pd.concat([df,table])
        i=i+1
    
    return df

#defines a function to extract data from API endpoint
def extracting_data_from_api(endpoint):
    """
    Parameters
    ----------
    endpoint: name of the endpoint from the config.ini file
    pages: number of pages get from previous function
    """
    ##Extracting data from the planned_tasks API
    #Creates a new dataframe to store the tasks
    df = pd.DataFrame() 
    #Extracts data from API endpoint
    r = requests.get(endpoint+f'?per-page=200', headers=headers)
    table = pd.DataFrame(r.json(),index=list(range(len(r.json()))))
    df = pd.concat([df,table])
    
    return df
    
#defines a function to extract data from API endpoint
def extracting_data_from_people_api(endpoint,pages):
    """
    Parameters
    ----------
    endpoint: name of the endpoint from the config.ini file
    pages: number of pages get from previous function
    """
    ##Extracting data from the planned_tasks API
    #Creates a new dataframe to store the tasks
    df = pd.DataFrame() 
    #Starts looping over the API endpoint
    i=1
    while i<=int(pages):
        #print(f"Page {i} from {pages}")
        #Iterates over the tasks endpoint extracting data for the required time range    
        r = requests.get(endpoint+f'?per-page=200&page={i}', headers=headers)
        table = pd.DataFrame(pd.json_normalize(r.json()),index=list(range(len(r.json()))))
        df = pd.concat([df,table])
        i=i+1
        
    return df

#Defining functions to extract and parse data from API
def extract_from_logged_time_api(endpoint, pages):
    """
    Parameters
    ----------
    endpoint: name of the endpoint from the config.ini file
    pages: number of pages get from previous function
    """
    ##Extracting data from the planned_tasks API
    #Creates a new dataframe to store the tasks
    df = pd.DataFrame() 
    #Starts looping over the API endpoint
    i=1
    while i<=int(pages):
        print(f"Page {i} from {pages}")
        print(endpoint)
        #Iterates over the tasks endpoint extracting data for the required time range    
        r = requests.get(endpoint+f'&page={i}', headers=headers)
        print(endpoint+f'&page={i}')
        table = pd.DataFrame(pd.json_normalize(r.json()),index=list(range(len(r.json()))))
        df = pd.concat([df,table])
        i=i+1
    
    return df

#Defining functions to extract and parse data from API
def extract_from_reports_api(endpoint, start_date, end_date):
    ##Performs the call to API to get logged time data of the previous month
    parameters = { 
            'start_date' : start_date,
            'end_date' : end_date,
            'per-page':'200'
                    }
    headers= {'Accept': accept_format, 'Authorization': auth_token}

    #Creates a dataframe to store logged time data of the previous month extracted from API
    df = pd.DataFrame()

    #Extracts data from AIP endpoint
    r = requests.get(endpoint, params=parameters, headers=headers)
    table = pd.DataFrame(pd.json_normalize(r.json()['people']),index=list(range(len(r.json()['people']))))

    return table


#creates function to upload delta data
def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

#Defining method to load data to postgres table
def load_data_to_postgres_no_pkey(df,table_name):
    """
    Parameters
    ----------
    df: name of the dataframe that has the data extracted from API endpoint
    table_name: name of the postgres table
    """
    # Loads data to PostgreSQL database
    
    try:
        #loads data if the table has a pkey using the postgres_upsert method
        df.to_sql(table_name, con=engine, if_exists='append', index=False)    

    except:
        print('Data was not loaded to the database')

#Defining method to load data to postgres table
def load_data_to_postgres_with_pkey(df,table_name,sql_file=None):
    """
    Parameters
    ----------
    df: name of the dataframe that has the data extracted from API endpoint
    table_name: name of the postgres table
    """
    # Loads data to PostgreSQL database
    
    try:
        #loads data if the table has a pkey using the postgres_upsert method
        df.to_sql(table_name, con=engine, if_exists='append', index=False, method=postgres_upsert)
        if sql_file is not None:
            fd = open(f'./sql/{sql_file}', 'r')
            sqlFile = fd.read()
            fd.close()
            sqlFile = sqlFile.split(";")
            sqlFile.pop()
            connection = psycopg2.connect(host=host, database=dbName, user='postgres', password=pwd)
            connection.autocommit=True
            cursor = connection.cursor()

            for query in sqlFile:
                cursor.execute(query)    
    
    except Exception as e:
        print(f'Data was not loaded to the database {e}')

#Defining function to check records from database
def records_check(table_name):
    """
    Parameters
    ----------
    table_name: name of the table in postgres
    """
    #Checks records on the corresponding table in Postgres
    check_exist = pd.read_sql_query(f"SELECT EXISTS(select * from {table_name} limit 1)", engine)
    
    if check_exist.exists[0]:
        check = pd.read_sql_query(f"SELECT * FROM {table_name}",engine)
        number_of_records = check.shape[0]
    else:
        print('Connection was not established with database')

    return number_of_records

def records_delete(table_name):
    conn = f"postgresql+psycopg2://postgres:{pwd}@{host}:{port}/{dbName}"
    engine = db.create_engine(conn,executemany_mode='values', executemany_values_page_size=50000)
    #Checks records on the corresponding table in Postgres
    check_exist = pd.read_sql_query(f"SELECT EXISTS(select * from {table_name} limit 1)", engine)
    check = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name} WHERE date_part('year', date)={current_year} AND date_part('month', date)>={current_month}", engine)
    number_of_records = int(check['count'])

    #Deletes records corresponding to current year and month from DB
    if check_exist.exists[0]:
        delete_query = f"DELETE FROM {table_name} WHERE date_part('year', date)={current_year} AND date_part('month', date)>={current_month}"
        engine.execute(delete_query) # to delete rows using a sqlalchemy function
        print(f'{number_of_records} records from {current_year} were deleted from {table_name}')
    else:
        print('Connection was not established with database')

    return number_of_records

def records_delete_reports(table_name):
    conn = f"postgresql+psycopg2://postgres:{pwd}@{host}:{port}/{dbName}"
    engine = db.create_engine(conn,executemany_mode='values', executemany_values_page_size=50000)
    #Checks records on the corresponding table in Postgres
    check_exist = pd.read_sql_query(f"SELECT EXISTS(select * from {table_name} limit 1)", engine)
    #check = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name} WHERE year={current_year} and month={current_month}", engine)
    check = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name} WHERE year = {current_year}", engine)
    number_of_records = int(check['count'])

    #Deletes records corresponding to current year and month from DB
    if check_exist.exists[0]:
        delete_query = f"DELETE FROM {table_name} WHERE year = {current_year}"
        engine.execute(delete_query) # to delete rows using a sqlalchemy function
        print(f'Records from {current_year} were deleted from {table_name}')
    else:
        print('Connection was not established with database')

    return number_of_records

def records_delete_tasks(table_name):
    conn = f"postgresql+psycopg2://postgres:{pwd}@{host}:{port}/{dbName}"
    engine = db.create_engine(conn,executemany_mode='values', executemany_values_page_size=50000)
    #Checks records on the corresponding table in Postgres
    check_exist = pd.read_sql_query(f"SELECT EXISTS(select * from {table_name} limit 1)", engine)
    check = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name} WHERE date_part('year',end_date) >= {current_year}", engine)
    number_of_records = int(check['count'])

    #Deletes records corresponding to current year and month from DB
    if check_exist.exists[0]:
        delete_query = f"DELETE FROM {table_name} WHERE date_part('year',end_date) >= {current_year}"
        engine.execute(delete_query) # to delete rows using a sqlalchemy function
        print(f'Records from {current_year} were deleted from {table_name}')
    else:
        print('Connection was not established with database')

    return number_of_records

def records_delete_slack(table_name):
    conn = f"postgresql+psycopg2://postgres:{pwd}@{host}:{port}/{dbName}"
    engine = db.create_engine(conn,executemany_mode='values', executemany_values_page_size=50000)
    #Checks records on the corresponding table in Postgres
    check_exist = pd.read_sql_query(f"SELECT EXISTS(select * from {table_name} limit 1)", engine)
    check = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name}", engine)
    number_of_records = int(check['count'])

    #Deletes records corresponding to current year and month from DB
    if check_exist.exists[0]:
        delete_query = f"DELETE FROM {table_name}"
        engine.execute(delete_query) # to delete rows using a sqlalchemy function
        print(f'Records were deleted from {table_name}')
    else:
        print('Connection was not established with database')

    return number_of_records

#Defining function to extract file in csv format to outputs folder
def extract_data_to_csv(df,filename,path=out_path):
    """
    Parameters
    ----------
    df : name of the dataframe
    filename :  name of the output file
    path: path the output files are stored
    """
    df.to_csv(path+filename+'.csv',sep=',',index=False)

#Defining function to extract file in excel format to outputs folder
def extract_data_to_excel(df,filename,path=out_path):
    """
    Parameters
    ----------
    df: name of the dataframe
    filename: name of the output file
    """
    df.to_excel(path+filename+'.xlsx',index=False)

#Defining function that sends slack notification to bla-reporting channel
def slack_notification(message, to="#bla-reporting"):
    try:
        slack_message = {'text': message, 
                         'username': 'float-pipelines', 
                         'icon_url': 'https://slack.com/img/icons/app-57.png',
                         'channel': to}

        response = requests.post(url = webhook_url,
                                data = json.dumps(slack_message),
                                headers = {'Content-Type': 'application/json'})
    except:
        traceback.print_exc()

    return True

# Put users into the dict
def save_slack_users(users_array):

    row = pd.DataFrame()
    df = pd.DataFrame()
    for user in users_array:
        # Key user info on their unique user ID
        row["user_id"] = [user["id"]]
        row["display_name"] = [user["profile"]["display_name"]]
        row["real_name"] = [user["profile"]["real_name"]]
        #row["real_name"] = [user["profile"]["real_name_normalized"]]
        df = pd.concat([df, row], axis=0)
    
    return df

def load_slack_users(df, table_name):
    #save data to slack_users table using upsert method
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    #print(df)

def no_logged_users_weekly_reminder():
    sql_query = "with pt as ( select pt.people_id, pt.task_date, TRANSLATE(pt.name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') as name, pt.year, pt.month, pt.day, sum(pt.hours) as hours \
                          from public.planned_tasks_view pt \
                          where pt.task_date <= CURRENT_DATE - 1 \
                          and pt.month = date_part('month',CURRENT_DATE) \
                          and pt.year >= 2023 \
                          group by 1,2,3,4,5,6), \
                                rt as ( select rtv.people_id, rtv.year, rtv.month, rtv.day, sum(rtv.hours) as hours \
                                        from reported_tasks_view rtv \
                                        where rtv.type = 'Reported' and rtv.year >= 2023 \
                                        group by 1,2,3,4 ), \
                                        pr as ( select p.people_id, p.active, p.end_date from people_raw p ) \
                                            select su.user_id, trim(TO_CHAR(cast(pt.task_date as TIMESTAMP), 'Month')) as month, pt.name, pr.active, sum(pt.hours) - sum(coalesce(rt.hours,0)) as no_logged \
                                            from pt \
                                            left join slack_users su on TRANSLATE(su.real_name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') = TRANSLATE(pt.name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') \
                                            left join rt on rt.people_id = pt.people_id and rt.month=pt.month and rt.year =pt.year and rt.day = pt.day \
                                            left join pr on pr.people_id = pt.people_id \
                            where TRANSLATE(pt.name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') in ('Felix Gutierrez','Gonzalo Bottari') \
                                            group by 1,2,3,4 \
                                            having sum(pt.hours) - sum(coalesce(rt.hours,0)) > 0 \
                                            order by 4 desc;"
    
    df = pd.read_sql_query(sql_query, engine)
    
    try:
        #Iterating on the dataframe
        for index, row in df.iterrows():
            message = f"Hey! :wave::skin-tone-2: We noticed you have {row['no_logged']} hours left pending to report in {row['month']}. Click on https://ballastlane.float.com/log-time and report it please, Thanks!"
            #Sending notifications to slack users
            slack_notification(message, row['user_id'])
            #Saving notifications sent in the log file /home/ubuntu/logs/{date}weekly-execution.log
            print(f"Notification sent to {row['name']}, Slack user_id:{row['user_id']},hours pending to log: {row['no_logged']}")
            
    except Exception as e:
        # You will get a SlackApiError if "ok" is False
        print(f"Got an error: {e}")
    
    return df

def no_logged_users_monthly_reminder():
    sql_query = "with pt as ( select pt.people_id, pt.task_date, TRANSLATE(pt.name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') as name, pt.year, pt.month, pt.day, sum(pt.hours) as hours \
                          from public.planned_tasks_view pt \
                          where pt.task_date <= CURRENT_DATE - 1 \
                          and pt.month = date_part('month',CURRENT_DATE -1) \
                          and pt.year >= 2023 \
                          group by 1,2,3,4,5,6), \
                                rt as ( select rtv.people_id, rtv.year, rtv.month, rtv.day, sum(rtv.hours) as hours \
                                        from reported_tasks_view rtv \
                                        where rtv.type = 'Reported' and rtv.year >= 2023 \
                                        group by 1,2,3,4 ) \
                                            select su.user_id, trim(TO_CHAR(cast(pt.task_date as TIMESTAMP), 'Month')) as month, pt.name, sum(pt.hours) - sum(coalesce(rt.hours,0)) as no_logged \
                                            from pt \
                                            left join slack_users su on su.real_name = TRANSLATE(pt.name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') \
                                            left join rt on rt.people_id = pt.people_id and rt.month=pt.month and rt.year =pt.year and rt.day = pt.day \
            where TRANSLATE(pt.name,'áéíóúñÁÉÍÓÚÑ','aeiounAEIOUN') in ('Felix Gutierrez','Gonzalo Bottari') \
                                            group by 1,2,3 \
                                            having sum(pt.hours) - sum(coalesce(rt.hours,0)) > 0;"
    
    df = pd.read_sql_query(sql_query, engine)
    
    try:
        for index, row in df.iterrows():
            message = f"Hey! :wave::skin-tone-2: We noticed you have {row['no_logged']} hours left pending to report in {row['month']}. Click on https://ballastlane.float.com/log-time and report it please, Thanks!"
            #Sending notifications to slack users
            slack_notification(message, row['user_id'])
            #Saving notifications sent in the log file /home/ubuntu/logs/{date}weekly-execution.log
            print(f"Notification sent to {row['name']}, Slack user_id:{row['user_id']},hours pending to log: {row['no_logged']}")

    except Exception as e:
        # You will get a SlackApiError if "ok" is False
        print(f"Got an error: {e}")
    
    return df

def data_validation_people():
    sql_query = "with people_raw as ( select p.people_id, p.name, p.active \
                          from people_raw p ), \
                          slack as ( \
                                    select p2.user_id, TRANSLATE(p2.real_name,'áéíóúÁÉÍÓÚ','aeiouAEIOU') as real_name \
                                    from slack_users p2 \
                                    ) \
                                    select pr.people_id, pr.name, slack.user_id, slack.real_name, pr.active, \
                                        case \
                                            when pr.name=slack.real_name then 'True' \
                                            else 'False' \
                                        end	as diff \
                                    from people_raw pr \
                                    left join slack on slack.real_name = pr.name \
                                    where pr.active = 1 \
                                    and slack.real_name is null;"
    
    df = pd.read_sql_query(sql_query, engine)
    
    if df.shape[0] > 0:

        try:
            slack_id = 'U02A3TRHD0F'
            #UQ422QJLF 'Laura'
            for index, row in df.iterrows():
                print(row['people_id'])
                print(row['name'])
                message = f"Hey! We noticed that some names does not match between DB table (people_raw) and the real_name in slack users :man-facepalming:. We need to notify them to change his/her Slack real_name to match with his/her name in Float, his/her name is {row['name']}. Thanks!"
                slack_notification(message, slack_id)
                #slack_notification(message)
                print(f"{row['name']} {message}")
        except Exception as e:
            # You will get a SlackApiError if "ok" is False
            print(f"Got an error: {e}")
        
    else:
        pass

    return df
    
def data_validation_scheduled_hours():

    sql_query = "select pt.year, pt.month, sum(pt.hours) as scheduled \
                    from public.planned_tasks_view pt \
                    where pt.year = 2023 \
                    group by 1,2 \
                    order by 2 asc;"
                    
    df = pd.read_sql_query(sql_query, engine)
    
    return df

def data_validation_reported_hours():

    sql_query = "select date_part('year',ltr.date) as year,date_part('month',ltr.date) as month,sum(ltr.hours) as logged \
                    from public.logged_time_raw ltr \
                    where TO_CHAR(ltr.date, 'YYYY-MM-DD') > TO_CHAR(CURRENT_DATE - INTERVAL '61 days', 'YYYY-MM-DD') \
                    and TO_CHAR(ltr.date, 'YYYY-MM-DD') < TO_CHAR(CURRENT_DATE + INTERVAL '30 days', 'YYYY-MM-DD') \
                    group by 1,2 \
                    order by 2 asc;"
                    
    df = pd.read_sql_query(sql_query, engine)
    
    return df