import requests
import fake_useragent
from bs4 import BeautifulSoup as bs
import time
import pymysql
from config import host, user, password, database
import datetime
import cloudscraper

headers = {
    'user-agent': fake_useragent.UserAgent().random
}

scraper=cloudscraper.create_scraper()
def parse(urls):
    with open(urls, 'r', encoding='utf-8') as file:
        # соединяемся с базой данных
        try:
            connection = pymysql.connect(
                host='Mysql@rc1b-07e8c4s3eag32smg.mdb.yandexcloud.net',
                user='user2',
                password='password',
                database='db1',
                charset=''
            )
            print('Cоединение с базой данных успешно установлено')
            print('#' * 20)
        except Exception as ex:
            print('Соединение с базой данных неудачно.')
            return
        # итерируемся по всем ссылкам из файла
        count = 1
        ulrs_array = [url.strip('\n') for url in file]
        for link in ulrs_array:
            time.sleep(1)
            link = link.strip('\n')
            response = scraper.get(link, headers=headers)
            if response.status_code!=200:
                continue
            soup = bs(response.text, 'lxml')
            item_name = soup.find('h1', id='pagetitle').text
            item_prices_blocks = soup.find('div', class_='prices_block').findAll('table',
                                                                                 class_='table table--multi_prices')
            total_price = ''
            for price_column in item_prices_blocks:
                th = price_column.findAll('th')
                td = price_column.findAll('td')
            for price in td:
                if '₽' in price.text:
                    total_price+=price.text+'\n'
            print(total_price)
            item_article = soup.find('div', class_='article iblock').text.replace('Артикул:', '').replace('\n', '')
            availability = soup.find('span', class_='store_view').text.strip()
            cursor = connection.cursor()
            now = str(datetime.datetime.now())[:19]
            # создание таблицы
            # create_table_query = "CREATE TABLE `property_pars`(id int AUTO_INCREMENT," \
            #                              " type varchar(32)," \
            #                          " price varchar(32)," \
            #                          " description varchar(200)," \
            #                          " address varchar(200)," \
            #                          " bedroom_quantity varchar(10)," \
            #                          " bath_quantity varchar(10)," \
            #                          " square varchar(20)," \
            #                          " listed varchar(30)," \
            #                          " date varchar(30)," \
            #                          " url varchar(1000), PRIMARY KEY (id));"
            # cursor.execute(create_table_query)
            insert_query = f"INSERT INTO `pars` (name, item_price_1_piece, item_price_1_pack,item_article, availability, date, url) " \
                           f"VALUES ('{item_name}'," \
                           f"'{total_price}', '-', '{item_article}', '{availability}', '{now}','{link}');"
            cursor.execute(insert_query)
            connection.commit()
            print(f'Данные из ссылки номер {count} добавлены, осталось обойти {len(ulrs_array) - count} ссылок')
            count += 1


file_name = 'link.txt'
parse(file_name)
