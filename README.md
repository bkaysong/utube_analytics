# 유튜브 데이터를 활용한 데이터 분석
----------------------------------
- 팀명 : 3팀
- 팀원 : 김선민, 양정헌, 박영후, 송봉기
- 기간 : 2022.3.14 ~ 2022.4.1

## 프로젝트의 팀원별 역할
>- 김선민 : Elastic 클러스터링구성, Kibana시각화, 웹, ES->HDFS 코드, 로그스태시 설정
>- 양정헌 : Hadoop 구성,
>- 박영후 : 데이터 크롤링
>- 송봉기 : RabbitMQ 구성

## 데이터 분석의 목적
>- 최신 인기 동영상의 댓글, 좋아요, 채널 구독자 등의 영상 정보를 수집
>- 최신 인기 동영상이 되는 조건 및 공통점 그리고 인기 동영상들의 특징을 분석
>- 라이브 뉴스 채팅 분석을 통한 우리나라 최근 시사점 파악

## 데이터의 구성도
<img width="80%" src="https://user-images.githubusercontent.com/102707438/160992079-e6494b64-92f9-4d5c-aa29-b72a47747aff.png"/>

## 시스템 구성
시스템설명|CPU|메모리|HDD|버전|비고
---|---|---|---|---|---|
Django_server-Web service|1Core|1G|8G|0|EC2
Django_server-Web service|1Core|1G|8G|0|EC2
Python(crawling)-유튜브 실시간 채팅|6Core|10G|0|0|window
Python(crawling)-유투브 인기영상중 카테고리들|6Core|10G|0|0|window
RabbitMQ|2Core|8G|20G|3.6.10|VMware
Logstash|3Core|6G|20G|1:7.17.1-1|VMware
Elastic(Master)|2Core|8G|20G|7.17.1|VMware
Elastic(Data1)|8Core|16G|422G|7.17.1|컴퓨터
Elastic(Data2)|2Core|6G|100G|7.17.1|VMware
Kibana|4Core|8G|90G|7.17.1|VMware
HDFS(NameNode)|4Core|8G|200G|3.2.2|컴퓨터
HDFS(Worker1)|8Core|16G|550G|3.2.2|컴퓨터
HDFS(Worker2)|8Core|16G|424G|3.2.2|컴퓨터
HDFS(SecondNameNode)|4Core|8G|90G|3.2.2|VMware
Spark(MasterNode)|4Core|8G|200G|3.1.2|컴퓨터
Spark(SlaveNode1)|8Core|16G|550G|3.1.2|컴퓨터
Spark(SlaveNode2)|8Core|16G|550G|3.1.2|컴퓨터
Spark(SlaveNode3)|8Core|16G|424G|3.1.2|컴퓨터

## 프로그램의 설정파일

## 웹 프로젝트

## 분석코드
### `crawling_news.py`
- ytn news의 실시간 채팅창을 크롤링해서 rabbitmq의 news queue로 보냄.
- 채팅친 시간, 닉네임, 채팅내용을 데이터로 가져온다
### `crawling_video_comment.py`
- <비디오파트>
> 비디오 고유id 값, 크롤링한 시간, 제목, 조회수, 좋아요수, 채널명, 구독자수, 댓글갯수, 인기순위
> > 여기까지 rabbitmq의 video queue로 데이터가 들어간다.
- <댓글부분>
> 댓글작성자, 댓글내용, 댓글좋아요 수
> > 여기까지 rabbitmq의 comment queue로 데이터가 들어간다.
### `es_to_hdfs.py`
- 엘라스틱에서 24시간이 지난 데이터를 하둡으로 보낸다. 
- 현재시간을 기준으로 24시간이 지난 데이터는 엘라스틱에서 삭제되는 코드 첨가
### `hdfs_to_es.py`
- 하둡에 저장된 cold 데이터를 spark.read로 읽어서 데이터 프레임을 만들고, 
- 데이터 프레임을 정형해서 원하는 칼럼을 가진 데이터프레임을 json형식으로 엘라스틱에 전달
