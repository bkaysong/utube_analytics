import json
import time

import pytchat
import pafy
import pika

__url = '172.30.1.95'
__port = 5672
__vhost = 'boong'
__cred = pika.PlainCredentials('bong', 'qwer1234')
__queue = 'news'

pafy.set_api_key('AIzaSyAxP59_mV_ee0nA9BIuIvUJkZrfvky1bjg')

video_id = 'GoXPbGQl-uQ'

v = pafy.new(video_id)
title = v.title
author = v.author
published = v.published

print(title)
print(author)
print(published)

chat = pytchat.create(video_id=video_id)

while chat.is_alive():
    for c in chat.get().sync_items():
        print(f"{c.datetime} [{c.author.name}]- {c.message}")

        str1 = str(c.datetime)
        date_time = str1.replace(" ", "T")

        # UTC 서울 표준 표기법은 2022-03-30T09:10:05+09:00 형식이다.
        str2 = {
            'date': date_time + "+09:00",
            'nickname': str(c.author.name),
            'content': str(c.message),
        }

        conn = pika.BlockingConnection(pika.ConnectionParameters(__url, __port, __vhost, __cred))
        chan = conn.channel()
        chan.basic_publish(exchange='', routing_key=__queue, body=json.dumps(str2))

        chan.close()
        conn.close()

        time.sleep(1)
