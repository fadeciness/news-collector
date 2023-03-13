import sqlite3
from datetime import datetime

import requests as requests
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

def main():
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


if __name__ == '__main__':
    main()
    print("New events downloaded")
