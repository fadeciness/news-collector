import os
import sqlite3 as sl
import time

import telebot

con = sl.connect('news-collector.db')
T = os.getenv('TELETOKEN')
bot = telebot.TeleBot(T)

# CHANNEL_NAME = '@FadecinessNews'
CHANNEL_NAME = '@testfadeciness'

with con:
    res = con.execute("SELECT * FROM edisclosureru_history WHERE is_notification_send = 0 LIMIT 20")
    for row in res:
        # print(row)
        message = 'id: ' + str(row[0]) + '\n' + \
                  'Наименование компании: ' + str(row[4]) + '\n' + \
                      'Дата публикации: ' + str(row[1]) + '\n' + \
                  'Событие: ' + str(row[2]) + '\n' + \
                  'Ссылка: ' + str(row[3])
        bot.send_message(chat_id=CHANNEL_NAME, text=message)
        print('Сообщение отправлено: ' + str(message))
        con.execute("UPDATE edisclosureru_history SET is_notification_send = 1 WHERE id = " + str(row[0]))
        print('Таблица edisclosureru_history обновлена - id: ' + str(row[0]))
        time.sleep(10)

print('Program finished')
