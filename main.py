## imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import sqlite3


##helper functions
def replace_lv_characters_with_eng(latvian_text):
    latin_equivalents = {'ā': 'a', 'Ā': 'A', 'č': 'c', 'Č': 'C', 'ē': 'e', 'Ē': 'E', 'ī': 'i', 'Ī': 'I', 'ķ': 'k',
                         'Ķ': 'K', 'ļ': 'l', 'Ļ': 'L', 'ņ': 'n', 'Ņ': 'N', 'š': 's', 'Š': 'S',
                         'ū': 'u', 'Ū': 'U', 'ž': 'z', 'Ž': 'Z', 'ģ': 'g', 'Ģ': 'G'}
    latin_text = re.sub('[{}]'.format(''.join(latin_equivalents.keys())),
                        lambda match: latin_equivalents[match.group(0)], latvian_text)
    return latin_text


def split_district_and_street_address_into_2_strings(s):
    match = re.search(r'[A-Za-z][^A-Z]*', s)
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

    return df


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


def create_sql_table():
    conn = sqlite3.connect("local_db.db")
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


def write_df_to_sql_table(df, db_name='local_db.db', table_name='ss_flat_sales'):
    conn = sqlite3.connect(db_name)
    df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
    conn.close()


def query_sql_table_save_to_df(db_name='local_db.db', table_name='ss_flat_sales'):
    conn = sqlite3.connect(db_name)
    sql = 'select * from ' + table_name
    #print(sql)
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df


def ss_parser(perform_printing=False):
    #url='https://www.ss.lv/msg/lv/real-estate/flats/riga/aplokciems/bnlekm.html'
    #url = 'https://www.ss.lv/lv/real-estate/flats/riga/imanta/sell/'
    #url = 'https://www.ss.lv/lv/real-estate/flats/riga/imanta/sell/page2.html'
    #url = 'https://www.ss.lv/lv/real-estate/flats/riga/aplokciems/sell/'
    #url = 'https://www.ss.lv/lv/real-estate/flats/riga/all/sell/'
    #url = 'https://www.ss.lv/lv/real-estate/flats/riga/centre/sell/'
    url = 'https://www.ss.lv/lv/real-estate/flats/riga/all/sell/page1'
    url_base = 'https://www.ss.lv/lv/real-estate/flats/riga/all/sell/'


    all_urls = get_all_eligible_urls_to_parse(url_base=url_base)#, hard_coded_last_url_page_number=2, do_printing=True)#, do_printing=True, hard_coded_last_url_page_number=5)
    if perform_printing: print(all_urls)
    all_dfs = []
    for single_url in all_urls:
        if perform_printing: print(single_url)
        df = parse_single_url_html_and_save_data_to_df(single_url, print_data=False)
        all_dfs.append(df)
        #print(df)
    #print(all_dfs)
    whole_df = pd.concat(all_dfs)
    if perform_printing:
        print(whole_df.head(3))
        print(whole_df[['room_cnt', 'm2', 'price', 'price_per_m2']].head(3))
        print('shape ' + str(whole_df.shape))
    write_df_to_sql_table(whole_df, db_name='local_db.db', table_name='ss_flat_sales')

    if perform_printing:
        t_df = query_sql_table_save_to_df()
        print(t_df)


## main
def main():
    ss_parser(perform_printing=False)


if __name__ == '__main__':
    main()

