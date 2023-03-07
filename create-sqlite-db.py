import logging
import sqlite3 as sl


def main():
    con = sl.connect('news-collector.db')

    # TABLE: tracked companies from e-disclosure.ru
    logging.debug("Database initializing")
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
    logging.debug("Table 'edisclosureru' created")

    with con:
        con.execute("DELETE FROM edisclosureru")
    logging.debug("Table 'edisclosureru' cleaned out")

    sql = 'INSERT INTO edisclosureru (shortname, ticker, inn, site_company_id) VALUES (?, ?, ?, ?)'
    data = [
        ('ПАО Газпром нефть', "MOEX:SIBN", "5504036333", "347"),
        ('ПАО Сбербанк', "MOEX:SBER", "7707083893", "3043"),
        ('ПАО Группа Черкизово', "MOEX:GCHE", "7718560636", "6652"),
        ('ПАО ФосАгро', "MOEX:PHOR", "7736216869", "573"),
        ('ПАО НК «Роснефть', "MOEX:ROSN", "7706107510", "6505"),
        ('ПАО Полюс', "MOEX:PLZL", "7703389295", "7832"),
        ('ПАО ММК', "MOEX:MAGN", "7414003633", "9"),
        ('ПАО Газпром', "MOEX:GAZP", "7736050003", "934"),
        ('ПАО АК АЛРОСА', "MOEX:ALRS", "1433000147", "199"),
        ('АО Позитив Текнолоджиз', "MOEX:POSI", "7718668887", "38196"),
        ('Fix Price Group PLC', "MOEX:FIXP", "", "38370"),
        ('ПАО ММЦБ', "MOEX:GEMA", "7736317497", "37646"),
        ('ПАО Пермэнергосбыт', "MOEX:PMSB", "5904123809", "7344"),
        ('ПАО ГМК "Норильский никель', "MOEX:GMKN", "8401005730", "564"),
        ('ПАО Распадская', "MOEX:RASP", "4214002316", "942"),
        ('ПАО Банк "Санкт-Петербург', "MOEX:BSPB", "7831000027", "3935"),
        ('ПАО Сегежа Групп', "MOEX:SGZH", "9703024202", "38038"),
        ('ПАО Центральный телеграф', "MOEX:CNTL", "7710146208", "369"),
        ('ПАО МТС', "MOEX:MTSS", "7740000076", "236"),
        ('ПАО Ростелеком', "MOEX:RTKM", "7707049388", "141"),
        ('ПАО ОГК-2', "MOEX:OGKB", "2607018122", "7234"),
        ('ПАО Магнит', "MOEX:MGNT", "2309085638", "7671"),
        ('ПАО Транснефть', "MOEX:TRNFP", "7706061801", "636"),
        ('ПАО Мосэнерго', "MOEX:MSNG", "7705035012", "936"),
        ('ПАО Корпорация ВСМПО-АВИСМА', "MOEX:VSMO", "6607000556", "1641")
    ]
    with con:
        con.executemany(sql, data)
    logging.debug("Table 'edisclosureru' filled in")

    with con:
        logging.debug("Table 'edisclosureru':")
        data = con.execute("SELECT * FROM edisclosureru")
        for row in data:
            logging.debug(row)

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
    logging.debug("Table 'edisclosureru_history' created")

    with con:
        con.execute("DELETE FROM edisclosureru_history")
    logging.debug("Table 'edisclosureru_history' cleaned out")

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
    logging.debug("Table 'tmp_edisclosureru_history' created")

    with con:
        con.execute("DELETE FROM tmp_edisclosureru_history")
    logging.debug("Table 'tmp_edisclosureru_history' cleaned out")


if __name__ == '__main__':
    main()
