import sqlite3 as sl


def main():
    con = sl.connect('news-collector.db')

    # TABLE: tracked companies from e-disclosure.ru
    print("Database initializing")
    with con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS edisclosureru (
                id INTEGER PRIMARY KEY,
                shortname VARCHAR(255),
                ticker VARCHAR(30),
                inn VARCHAR(12),
                site_company_id VARCHAR(255)
            );
        """)
    print("Table 'edisclosureru' created")

    with con:
        con.execute("DELETE FROM edisclosureru")
    print("Table 'edisclosureru' cleaned out")

    sql = 'INSERT INTO edisclosureru (shortname, ticker, inn, site_company_id) VALUES (?, ?, ?, ?)'
    data = [
        ('ПАО "Газпром нефть"', "MOEX:SIBN", "5504036333", "347"),
        ('ПАО Сбербанк', "MOEX:SBER", "7707083893", "3043")
    ]
    with con:
        con.executemany(sql, data)
    print("Table 'edisclosureru' filled in")

    with con:
        print("Table 'edisclosureru':")
        data = con.execute("SELECT * FROM edisclosureru")
        for row in data:
            print(row)

    # TABLE: history of articles
    with con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS edisclosureru_history (
                id INTEGER PRIMARY KEY,
                publication_date VARCHAR(30) NOT NULL,
                event_name VARCHAR(255) NOT NULL,
                event_url VARCHAR(255) NOT NULL,
                shortname VARCHAR(255) NOT NULL,
                site_company_id VARCHAR(255) NOT NULL,
                is_notification_send INTEGER DEFAULT 0 NOT NULL,
                FOREIGN KEY(shortname) REFERENCES edisclosureru(shortname),
                FOREIGN KEY(site_company_id) REFERENCES edisclosureru(site_company_id)
            );
        """)
    print("Table 'edisclosureru_history' created")

    with con:
        con.execute("DELETE FROM edisclosureru_history")
    print("Table 'edisclosureru_history' cleaned out")

    # TEMPORARY TABLE: tmp history of articles
    with con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS tmp_edisclosureru_history (
                publication_date VARCHAR(30) NOT NULL,
                event_name VARCHAR(255) NOT NULL,
                event_url VARCHAR(255) NOT NULL,
                shortname VARCHAR(255) NOT NULL,
                site_company_id VARCHAR(255) NOT NULL,
                FOREIGN KEY(shortname) REFERENCES edisclosureru(shortname),
                FOREIGN KEY(site_company_id) REFERENCES edisclosureru(site_company_id)
            );
        """)
    print("Table 'tmp_edisclosureru_history' created")

    with con:
        con.execute("DELETE FROM tmp_edisclosureru_history")
    print("Table 'tmp_edisclosureru_history' cleaned out")


if __name__ == '__main__':
    main()
