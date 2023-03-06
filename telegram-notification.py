import logging
import os
import sqlite3 as sl
import time

import telebot


def main():
    con = sl.connect('news-collector.db')
    telegram_token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    bot = telebot.TeleBot(telegram_token)

    with con:
        res = con.execute("SELECT * FROM edisclosureru_history WHERE is_notification_send = 0 LIMIT 10")
        for row in res:
            message = 'id: ' + str(row[0]) + '\n' + \
                      'Наименование компании: ' + str(row[4]) + '\n' + \
                      'Дата публикации: ' + str(row[1]) + '\n' + \
                      'Событие: ' + str(row[2]) + '\n' + \
                      'Ссылка: ' + str(row[3])
            bot.send_message(chat_id=chat_id, text=message)
            logging.debug('Сообщение отправлено: ' + str(message))
            con.execute("UPDATE edisclosureru_history SET is_notification_send = 1 WHERE id = " + str(row[0]))
            time.sleep(10)


if __name__ == '__main__':
    main()
    logging.debug("Notification sent")
