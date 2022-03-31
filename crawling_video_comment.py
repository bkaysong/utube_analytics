from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import datetime
import requests
import bs4

import json
import pika

__url = '172.30.1.95'
__port = 5672
__vhost = 'boong'
__cred = pika.PlainCredentials('bong', 'qwer1234')
__queue1 = 'video'
__queue2 = 'comment'

rank = 1

driver=webdriver.Chrome('c:/chromedriver.exe')

driver.get("https://www.youtube.com/feed/trending")

time.sleep(5)

# 인기 카테고리 가져오기(최신 음악 게임 영화)
tab_lists = driver.find_elements(by= By.TAG_NAME, value='tp-yt-paper-tab')

tenThousand = False
thousand = False
point = False

for tab_list in tab_lists:
    tab_list.click()
    time.sleep(3)

    # 영상 리스트 가져오기
    video_lists = driver.find_elements(by=By.TAG_NAME, value='ytd-video-renderer')

    rank = 1

    for video_list in video_lists:

        video_id = video_list.find_element(by=By.TAG_NAME, value='a').get_attribute('href')
        video_id = str(video_id)
        video_id = video_id.strip().split("=")
        video_id = video_id[1]

        new_tab = video_list.find_element(by=By.TAG_NAME, value='a')
        new_tab.send_keys(Keys.CONTROL + "\n")

        # 새로운 탭으로 이동
        driver.switch_to.window(driver.window_handles[1])

        time.sleep(3)

        # 동영상 정보 긁어오는 날짜 시간
        now = datetime.datetime.now()
        now = str(now)
        now = now.replace(" ", "T")
        now = now.strip().split(".")
        time_now = now[0]
        time_now = time_now + "+09:00" # UTC 서울 표준 표기법은 2022-03-30T09:10:05+09:00 형식이다.

        # 동영상 제목, 조회수
        doru = requests.get(driver.current_url)
        doru_text = bs4.BeautifulSoup(doru.text, 'lxml')

        title = doru_text.select_one('meta[itemprop="name"][content]')['content']
        view = doru_text.select_one('meta[itemprop="interactionCount"][content]')['content']

        # 좋아요
        likes = driver.find_elements(by=By.ID, value='text')
        like = likes[2].get_attribute('aria-label')
        try:

            try:
                like = like.replace('개', '')
            except:
                print("no")

            try:
                like = like.replace(',', '')
            except:
                print('no')

            like = like.strip().split(' ')
            like = like[1]
        except:
            like = '0'

        # 채널 명
        channel = str(likes[7].text)

        # 구독자 수
        subscribers = driver.find_elements(by=By.ID, value='upload-info')

        try:
            subscriber = subscribers[1].text

            subscriber = subscriber.replace("만명", '')
            subscriber = subscriber.strip().split('\n')
            subscriber = subscriber[1]
            subscriber = subscriber.strip().split(' ')
            subscriber = subscriber[1]

            rest_true = True

            for k in subscriber:
                if k == '.':
                    subscriber = subscriber.replace('.', '')
                    subscriber = subscriber + '000'
                    rest_true = False
                    break

                if k == '명':
                    subscriber = subscriber.replace('명', '')
                    rest_true = False

            if rest_true:
                subscriber = subscriber + '0000'

        except:
            subscriber = "0"

        # 댓글 수
        binary = 'C:\chromedriver.exe'
        browser = webdriver.Chrome(binary)
        browser.get(driver.current_url)

        try:
            browser.maximize_window()

        except:
            continue

        time.sleep(2)
        browser.find_element_by_tag_name('html').send_keys(Keys.PAGE_DOWN)
        time.sleep(2)
        html = browser.page_source
        soup = bs4.BeautifulSoup(html, "lxml")
        num = soup.find_all('yt-formatted-string', class_='count-text style-scope ytd-comments-header-renderer') # 댓글 665개

        try:
            comment_num = num[0].get_text()[3:-1] # 665

        except:
            comment_num = '0'

        try:
            comment_num = comment_num.replace(',', '')
        except:
            comment_num = comment_num

        #영상에서 댓글 긁어 오기
        n = 1000
        while (n > 0):
            time.sleep(2)
            try:
                browser.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            except:
                break
            n -= 20

        time.sleep(2)
        html = browser.page_source
        soup = bs4.BeautifulSoup(html, "lxml")
        comment = soup.find_all('yt-formatted-string', class_='style-scope ytd-comment-renderer')
        user = soup.find_all('span', class_='style-scope ytd-comment-renderer')
        likes = soup.find_all('span', class_='style-scope ytd-comment-action-buttons-renderer')

        user_lists = []
        comment_lists = []
        likes_lists = []

        for i in user:
            if i.get_text() != '''\n''':
                i_text = i.get_text().strip().split('•')

                if len(i_text[0]) > 0:
                    user_lists.append(i_text[0])

        for i in comment:
            if i.get_text() != '''\n''':
                comment_lists.append(i.get_text())

        for i in likes:
            if i.get_text() != '''\n''':
                i_text = i.get_text().strip()

                # 오류 : ValueError: invalid literal for int() with base 10: '2만'
                # 해결 : 좋아요 변수의 '만'이라는 글자를 처리할 변수 tenThousand를 선언하고 처리식을 포함시켰다.

                tenThousand = False
                thousand = False
                point = False
                peple = False


                for j in i_text:
                    if j == '만':
                        tenThousand = True

                    if j == '천':
                        thousand = True

                    if j == '.':
                        point = True

                    if j == '명':
                        peple = True

                if tenThousand:
                    i_text = i_text.replace('만', '')

                    if point:
                        i_text = i_text.replace('.', '')
                        i_text = i_text + '000'

                    else:
                        i_text = i_text + '0000'

                if thousand:
                    i_text = i_text.replace('천', '')

                    if point:
                        i_text = i_text.replace('.', '')
                        i_text = i_text + '00'

                    else:
                        i_text = i_text + '000'

                if peple:
                    i_text = i_text.replace('명', '')

                likes_lists.append(i_text)

        # 추가 : likes는 int로 받아야해서 int로 형변환을 하였다.

        numb = len(user_lists)

        if numb > len(comment_lists):
            numb = len(comment_lists)

            if numb > len(likes_lists):
                numb = len(likes_lists)

        elif numb > len(likes_lists):
            numb = len(likes_lists)

            if numb > len(comment_lists):
                numb = len(likes_lists)

        for i in range(0, numb):
            comment_str = {
                'video_id': video_id,
                'user': user_lists[i],
                'content': comment_lists[i],
                "likes": int(likes_lists[i])
            }

            print(comment_str)

            conn = pika.BlockingConnection(pika.ConnectionParameters(__url, __port, __vhost, __cred))
            chan = conn.channel()
            chan.basic_publish(exchange='', routing_key=__queue2, body=json.dumps(comment_str))

            chan.close()
            conn.close()

        # int로 매핑 되어있는 key값의 value는 int로 형변환 하였다.
        video_str = {
            'video_id': video_id,
            'crawl_date': time_now,
            'title': title,
            'views': int(view),
            'likes': int(like),
            'channel': channel,
            'subscribers': int(subscriber),
            'comments': int(comment_num),
            'rank': rank
        }

        print(video_str)

        conn = pika.BlockingConnection(pika.ConnectionParameters(__url, __port, __vhost, __cred))
        chan = conn.channel()
        chan.basic_publish(exchange='', routing_key=__queue1, body=json.dumps(video_str))


        chan.close()
        conn.close()

        # browser.quit()

        # 현재 브라우저만 닫기
        driver.close()

        # 메인 탭으로 이동
        driver.switch_to.window(driver.window_handles[0])

        rank += 1

time.sleep(5)

#모든 브라우저 닫기
driver.quit()