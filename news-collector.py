import datetime
import sqlite3 as sl

import requests as requests
from bs4 import BeautifulSoup


def main():
    edisclosureru_company_news_url_prefix = 'https://www.e-disclosure.ru/Event/Page?companyId='

    con = sl.connect('news-collector.db')
    with con:
        companies_info = con.execute("SELECT id, shortname, inn, site_company_id FROM edisclosureru")

    current_year = datetime.date.today().year
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0',
    }
    for company in companies_info:
        request_url = edisclosureru_company_news_url_prefix + company[3] + '&year=' + str(current_year)
        print("Request URL: " + request_url)
        response = requests.get(url=request_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = []
        for table_row in soup.findAll('tr')[1:]:
            cols = table_row.find_all('td')
            publication_date = cols[1].text.replace(u'\xa0', ' ')
            event_name = cols[2].text.replace('\n', '')
            company_short_name = company[1]
            site_company_id = company[3]
            event_url = cols[2].find('a')['href'].replace('\'', '')
            data.append((publication_date, event_name, event_url, company_short_name, site_company_id))

        sql_insert = """
            INSERT INTO tmp_edisclosureru_history 
                (publication_date, event_name, event_url, shortname, site_company_id) 
            VALUES (?, ?, ?, ?, ?);
        """
        with con:
            con.executemany(sql_insert, data)

        with con:
            # Update information in edisclosureru_history table
            con.execute("""
                INSERT INTO edisclosureru_history (publication_date, event_name, event_url, shortname, site_company_id)
                    SELECT publication_date, event_name, event_url, shortname, site_company_id FROM tmp_edisclosureru_history
                EXCEPT
                    SELECT publication_date, event_name, event_url, shortname, site_company_id FROM edisclosureru_history
            """)

        with con:
            con.execute("DELETE FROM tmp_edisclosureru_history")


if __name__ == '__main__':
    main()
    print("New events downloaded")
