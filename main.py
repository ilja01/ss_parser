## imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import sqlite3
#import psycopg2
#from sqlalchemy import create_engine
import sqlalchemy
import csv
import datetime
import os
from telegram import Bot
#from telegram.constants import ParseMode
import asyncio

## Helper functions
## DF processing
def replace_lv_characters_with_eng(latvian_text):
    latin_equivalents = {'ā': 'a', 'Ā': 'A', 'č': 'c', 'Č': 'C', 'ē': 'e', 'Ē': 'E', 'ī': 'i', 'Ī': 'I', 'ķ': 'k',
                         'Ķ': 'K', 'ļ': 'l', 'Ļ': 'L', 'ņ': 'n', 'Ņ': 'N', 'š': 's', 'Š': 'S',
                         'ū': 'u', 'Ū': 'U', 'ž': 'z', 'Ž': 'Z', 'ģ': 'g', 'Ģ': 'G'}
    latin_text = re.sub('[{}]'.format(''.join(latin_equivalents.keys())),
                        lambda match: latin_equivalents[match.group(0)], latvian_text)
    return latin_text


def split_district_and_street_address_into_2_strings(s):
    match = re.search(r'[A-Za-z][^A-Z0-9]*', s)
    if s[:3] == 'VEF':
        s1 = 'vef'
    elif s[:21] == 'Sampeteris-Pleskodale':
        s1 = 'sampeteris-pleskodale'
    elif match:
        s1 = match.group(0).lower()
    else:
        s1 = ""
    s2 = s[len(s1):].lower()
    return s1, s2


def process_df_columns(df):
    num_cols = ['room_cnt', 'm2', 'price']

    df['price'] = df['price_raw'].str.replace(',', '').str.extract('(\d+)')
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
    df['price_per_m2'] = (df['price'] / df['m2']).round(1)
    df['adress_latin'] = df['adress'].apply(replace_lv_characters_with_eng).apply(pd.Series)
    df[['district', 'street_address']] = df['adress_latin'].apply(
        split_district_and_street_address_into_2_strings).apply(pd.Series)
    df['link'] = 'https://www.ss.lv/' + df['link']
    timestamp = datetime.datetime.now()
    df['extr_time'] = timestamp
    return df


def prep_fresh_data_df(df):
    time_threshold = datetime.datetime.now() - datetime.timedelta(hours=20)
    fresh_set = df[(df['extr_time'] >= time_threshold) & (df['proj_type']=='Jaun.')]
    fresh_set = fresh_set.sort_values('price_per_m2', ascending=True)
    fresh_set = fresh_set.reset_index()
    fresh_set['n'] = fresh_set.index + 1
    return fresh_set


## html parsing
def parse_single_url_html_and_save_data_to_df(input_url, print_data=False):
    response = requests.get(input_url)

    # Parse the HTML content of the website
    soup = BeautifulSoup(response.content, 'html.parser')

    data = []
    for row in soup.find_all('tr'):
        cols = row.find_all('td')
        # link = cols.get('href')
        # links = row.find_all(class_='msg2')
        # links = [link.get('href') for link in links]
        link_class = row.find('a', class_='am')
        link = link_class['href'] if link_class else None
        ad_id = link_class['id'] if link_class else None

        cols = [col.text for col in cols]
        cols.append(link)
        cols.append(ad_id)
        if cols[0] == '' and cols[1] == '':
            data.append(cols[2:])
        # print(cols)
        # print(links)

    if print_data:
        for row in data:
            print('length:', len(row))
            print(row)

    # if specific district is chosen then we have price per m2 provided.
    # If all Riga districts are chosen, then price per m2 is not provided; however, in address district is also showns
    col_names = ['descr_txt', 'adress', 'room_cnt', 'm2', 'floor', 'proj_type', #'price_per_m2',
                 'price_raw', 'link', 'ad_id']
    # print(data)
    df = pd.DataFrame(data, columns=col_names)
    df = process_df_columns(df)

    return df


def find_last_url(url='https://www.ss.lv/lv/real-estate/flats/riga/all/sell/page1'):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    last_link_obj = soup.find(class_='navi', rel='prev')
    last_link = last_link_obj['href'] if last_link_obj else None
    # print(last_link)
    return last_link


def get_all_eligible_urls_to_parse(url_base='https://www.ss.lv/lv/real-estate/flats/riga/all/sell/',
                                   do_printing=False,
                                   hard_coded_last_url_page_number=None):

    if hard_coded_last_url_page_number is None:
        url_last = find_last_url(url_base)
        url_last_page_number = int(re.search(r'page(\d+)', url_last).group(1))
    else:
        url_last = None
        url_last_page_number = hard_coded_last_url_page_number

    urls = []
    if url_last is None and hard_coded_last_url_page_number is None:
        urls.append(url_base)
    else:
        if do_printing: print('url_last_page_number:', url_last_page_number)

        for i in range(1, url_last_page_number+1):
            url_i = url_base+'page'+str(i)+'.html'
            if do_printing: print('url_i:', url_i)
            urls.append(url_i)

    if do_printing: print(urls)

    return urls


## connecting, writing & reading to DB
def read_creds_from_csv(csv_file_name='db_creds.csv'):
    script_path = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(script_path, csv_file_name)
    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        credentials = next(reader)
    return credentials


def get_connection_to_db(use_psql=False):
    if use_psql:
        creds = read_creds_from_csv()
        db_name, user, password, host, port = creds['db_name'], creds['user'], creds['password'], creds['host'], creds['port']
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        #conn_str = f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db_name']}"
        engine = sqlalchemy.create_engine(conn_str)
        conn = engine.connect()
    else:
        conn = sqlite3.connect("local_db.db")

    return conn


def write_df_to_sql_table(df, table_name='ss_flat_sales', use_psql=False, perform_printing=False):
    #conn = sqlite3.connect(db_name)
    conn = get_connection_to_db(use_psql=use_psql)
    if perform_printing:
        print('conn: ')
        print(conn)
        print('df: ')
        print(df)

    df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
    if use_psql:
        conn.commit()
    conn.close()


def query_sql_table_save_to_df(table_name='ss_flat_sales', use_psql=True):
    conn = get_connection_to_db(use_psql=use_psql)
    #conn = sqlite3.connect(db_name)
    sql = 'select * from ' + table_name
    if use_psql:
        sql = sqlalchemy.text(sql)
    #print(sql)
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df


def create_sql_table(use_psql=False):
    conn = get_connection_to_db(use_psql=use_psql)
    cursor = conn.cursor()

    #'descr_txt', 'adress', 'room_cnt', 'm2', 'floor', 'proj_type',  # 'price_per_m2',
    #'price_raw', 'link', 'ad_id',     'price_per_m2'

    cursor.execute("""
    CREATE TABLE ss_flat_sales (
        id INTEGER PRIMARY KEY,
        descr_txt TEXT,
        adress TEXT,
        room_cnt INTEGER,
        m2 REAL,
        floor TEXT,
        proj_type TEXT,
        price_raw TEXT,
        link TEXT,
        ad_id TEXT,
        price REAL,
        price_per_m2 REAL        
    )
    """)

    conn.commit()
    conn.close()


## telegram
async def telegram_bot_send_text(text_message='hello world!'):
    creds = read_creds_from_csv(csv_file_name='telegram_creds.csv')
    bot_token, chat_id, chat_id2 = creds['bot_token'], creds['chat_id'], creds['chat_id2']
    bot = Bot(token=bot_token)
    #await bot.send_message(chat_id=chat_id, text=text_message)
    await bot.send_message(chat_id=chat_id2, text=text_message)


def print_df_via_telegram_bot(fresh_set):
    asyncio.run(telegram_bot_send_text('new offers:'))
    fresh_set['m2_price'] = round(fresh_set['price_per_m2'], 0).astype(int)
    fresh_set['room_cnt'] = round(fresh_set['room_cnt'], 0).astype(int)
    cols = ['n', 'price_raw', 'm2', 'room_cnt', 'm2_price']
    df_text1 = fresh_set[cols].to_string(index=False)
    asyncio.run(telegram_bot_send_text(df_text1))

    fresh_set['full_address'] = fresh_set['district'] + '; ' + fresh_set['street_address']
    df_text2 = fresh_set[['n', 'full_address']].to_string(index=False, header=True, justify='left')
    asyncio.run(telegram_bot_send_text(df_text2))

    df_text3 = fresh_set[['n', 'link']].to_string(index=False, header=False)
    asyncio.run(telegram_bot_send_text(df_text3))


## final function
def ss_parser(perform_printing=False, use_psql=True):
    url_base = 'https://www.ss.lv/lv/real-estate/flats/riga/all/sell/'

    all_urls = get_all_eligible_urls_to_parse(url_base=url_base)#, hard_coded_last_url_page_number=2, do_printing=True)#, do_printing=True, hard_coded_last_url_page_number=5)
    all_dfs = []
    for single_url in all_urls:
        df = parse_single_url_html_and_save_data_to_df(single_url, print_data=False)
        all_dfs.append(df)

    whole_df = pd.concat(all_dfs)
    existing_df = query_sql_table_save_to_df(use_psql=True)
    combined_df = pd.concat([existing_df, whole_df], sort=False)
    #combined_df = combined_df.sort_values('extr_time')
    combined_df = combined_df.drop_duplicates(subset=['ad_id','price','link','m2','room_cnt','floor','proj_type','adress_latin'], keep='first')

    write_df_to_sql_table(combined_df, use_psql=use_psql, #False, #db_name='local_db.db',
                          perform_printing=perform_printing,
                          table_name='ss_flat_sales')

    #asyncio.run(telegram_bot_send_text('ss flat data load to db completed!'))

    fresh_set = prep_fresh_data_df(combined_df)

    if fresh_set.shape[0] >= 1:
        print_df_via_telegram_bot(fresh_set)
    else:
        asyncio.run(telegram_bot_send_text('no new offers!'))


## main
def main():
    ss_parser(perform_printing=False)


if __name__ == '__main__':
    main()

#################
# some testing
def some_testing():
    df2=query_sql_table_save_to_df(use_psql=False)
    df2
    df3=query_sql_table_save_to_df(use_psql=True)
    df3

    creds = read_creds_from_csv()
    db_name, user, password, host, port = creds['db_name'], creds['user'], creds['password'], creds['host'], creds['port']
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = sqlalchemy.create_engine(conn_str)#, dialect='postgresql')#, is_async=False)
    ##conn = engine#.connect()

    conn = engine.connect()
    results = conn.execute(sqlalchemy.text('SELECT * FROM ss_flat_sales')).fetchall()
    results
    #conn.close()

    metadata = sqlalchemy.MetaData()
    test_table = sqlalchemy.Table('test_table', metadata, autoload_with=engine)

    # Insert data into the table
    conn.execute(sqlalchemy.text("insert into test_table(id, s) values (4, 'inserted from Python')"))
    conn.execute(test_table.insert().values(id=2, s='inserted from Python'))
    conn.commit()
    results2 = conn.execute(sqlalchemy.text('SELECT * FROM test_table')).fetchall()
    results2
    conn.close()

    # Get current timestamp
    timestamp = datetime.datetime.now()
    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    # Print timestamp
    print(timestamp_str)

    timestamp = datetime.datetime.now()

    t_data = {
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'age': [25, 30, 35, 40],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston']
    }

    # create a dataframe from the dictionary
    df = pd.DataFrame(t_data)
    df['extr_time'] = timestamp
    # display the dataframe
    print(df)