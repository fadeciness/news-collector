import os
import sqlite3
import time

import telebot


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


def main():
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
    main()
    print("Notification sent")
