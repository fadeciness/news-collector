import os
import sqlite3
import time
from datetime import datetime

import requests as requests
import telebot
from bs4 import BeautifulSoup


def get_companies_info():
    connection = None
    try:
        connection = sqlite3.connect('news-collector.db')
        cursor = connection.cursor()
        query_get_companies_info = "SELECT id, shortname, inn, site_company_id FROM edisclosureru"
        cursor.execute(query_get_companies_info)
        companies_info = cursor.fetchall()
        cursor.close()
        return companies_info
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if connection:
            connection.close()
            print("Соединение с SQLite закрыто")


def update_events_history(data):
    connection = None
    try:
        connection = sqlite3.connect('news-collector.db')
        cursor = connection.cursor()
        query_insert_all_events = """
            INSERT INTO tmp_edisclosureru_history 
                (publication_date, event_name, event_url, shortname, site_company_id) 
            VALUES (?, ?, ?, ?, ?);
        """
        count = cursor.executemany(query_insert_all_events, data)
        connection.commit()
        print("Общее количество загруженных событий: " + str(cursor.rowcount))
        cursor.close()

        cursor = connection.cursor()
        # Update information in edisclosureru_history table
        query_update_events_history = """
            INSERT INTO edisclosureru_history (publication_date, event_name, event_url, shortname, site_company_id)
                SELECT publication_date, event_name, event_url, shortname, site_company_id FROM tmp_edisclosureru_history
            EXCEPT
                SELECT publication_date, event_name, event_url, shortname, site_company_id FROM edisclosureru_history
            """
        count = cursor.execute(query_update_events_history)
        connection.commit()
        print("Количество новых событий: " + str(cursor.rowcount))
        cursor.close()

        cursor = connection.cursor()
        query_delete_events_tmp_table = "DELETE FROM tmp_edisclosureru_history"
        count = cursor.execute(query_delete_events_tmp_table)
        connection.commit()
        print("Временная таблица вычищена: " + str(cursor.rowcount))
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if connection:
            connection.close()
            print("Соединение с SQLite закрыто")


def fetch_new_events():
    edisclosureru_company_news_url_prefix = 'https://www.e-disclosure.ru/Event/Page?companyId='

    current_year = datetime.today().year
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0',
    }
    for company in get_companies_info():
        request_url = edisclosureru_company_news_url_prefix + company[3] + '&year=' + str(current_year)
        print("Request URL: " + request_url)
        response = requests.get(url=request_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = []
        for table_row in soup.findAll('tr')[1:]:
            cols = table_row.find_all('td')
            publication_date = cols[1].text.replace(u'\xa0', ' ')
            publication_date = datetime.strptime(publication_date, '%d.%m.%Y %H:%M').strftime('%Y-%m-%d %H:%M')
            event_name = cols[2].text.replace('\n', '')
            company_short_name = company[1]
            site_company_id = company[3]
            event_url = cols[2].find('a')['href'].replace('\'', '')
            data.append((publication_date, event_name, event_url, company_short_name, site_company_id))
        update_events_history(data)


def get_unprocessed_events():
    connection = None
    try:
        connection = sqlite3.connect('news-collector.db')
        cursor = connection.cursor()
        query_get_unprocessed_events = "SELECT * FROM edisclosureru_history WHERE is_notification_send = 0 ORDER BY publication_date LIMIT 10"
        cursor.execute(query_get_unprocessed_events)
        unprocessed_events = cursor.fetchall()
        cursor.close()
        return unprocessed_events
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if connection:
            connection.close()
            print("Соединение с SQLite закрыто")


def set_notification_status(event_id):
    connection = None
    try:
        connection = sqlite3.connect('news-collector.db')
        cursor = connection.cursor()
        query_set_notification_status = "UPDATE edisclosureru_history SET is_notification_send = 1 WHERE id = " + str(event_id)
        cursor.execute(query_set_notification_status)
        connection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if connection:
            connection.close()
            print("Соединение с SQLite закрыто")


def send_notification():
    telegram_token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    bot = telebot.TeleBot(telegram_token)

    for row in get_unprocessed_events():
        message = 'id: ' + str(row[0]) + '\n' + \
                  'Наименование компании: ' + str(row[4]) + '\n' + \
                  'Дата публикации: ' + str(row[1]) + '\n' + \
                  'Событие: ' + str(row[2]) + '\n' + \
                  'Ссылка: ' + str(row[3])
        bot.send_message(chat_id=chat_id, text=message)
        print('Сообщение отправлено: ' + str(message))
        set_notification_status(row[0])
        time.sleep(5)


if __name__ == '__main__':
    fetch_new_events()
    send_notification()
    print("Program finished")
