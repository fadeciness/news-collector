import datetime
import sqlite3 as sl

import requests as requests
from bs4 import BeautifulSoup

EDISCLOSURERU_COMPANY_URL_PREFIX = 'https://www.e-disclosure.ru/portal/company.aspx?id='
EDISCLOSURERU_COMPANY_NEWS_URL_PREFIX = 'https://www.e-disclosure.ru/Event/Page?companyId='

if __name__ == '__main__':
    con = sl.connect('news-collector.db')
    with con:
        companies_info = con.execute("SELECT id, shortname, inn, site_company_id FROM edisclosureru")

    year = datetime.date.today().year
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0',
    }
    for company in companies_info:
        requestUrl = EDISCLOSURERU_COMPANY_NEWS_URL_PREFIX + company[3] + '&year=' + str(year)
        print(requestUrl)
        response = requests.get(requestUrl, headers=headers)
        # print(response)
        soup = BeautifulSoup(response.text, 'html.parser')
        # print(soup)
        # with open("Output.html", "w", encoding='UTF-8') as text_file:
        #     text_file.write(response.text)
        data = []
        for table_row in soup.findAll('tr')[1:]:
            # print(table_row)
            cols = table_row.find_all('td')
            publication_date = cols[1].text.replace(u'\xa0', ' ')
            event_name = cols[2].text.replace('\n', '')
            company_short_name = company[1]
            site_company_id = company[3]
            event_url = cols[2].find('a')['href']
            data.append((publication_date, event_name, event_url, company_short_name, site_company_id))
        print(len(data))

        sql_insert = """
            INSERT INTO tmp_edisclosureru_history 
                (publication_date, event_name, event_url, shortname, site_company_id) 
            VALUES (?, ?, ?, ?, ?);
        """
        with con:
            con.executemany(sql_insert, data)

        # with con:
        #     print("Table 'tmp_edisclosureru_history'")
        #     data = con.execute("SELECT * FROM tmp_edisclosureru_history")
        #     for row in data:
        #         print(row)

        with con:
            print("Update information in edisclosureru_history table")
            con.execute("""
                INSERT INTO edisclosureru_history (publication_date, event_name, event_url, shortname, site_company_id)
                    SELECT publication_date, event_name, event_url, shortname, site_company_id FROM tmp_edisclosureru_history
                EXCEPT
                    SELECT publication_date, event_name, event_url, shortname, site_company_id FROM edisclosureru_history
            """)

        with con:
            con.execute("DELETE FROM tmp_edisclosureru_history")

    # with con:
    #     res = con.execute("SELECT * FROM edisclosureru_history LIMIT 7")
    #     for row in res:
    #         print(row)
