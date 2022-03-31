# 유투브 데이터를 활용한 데이터 분석
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
>- 라이브 뉴스 채팅 분석을 통한 우리나라 최근 시사점 파악

## 데이터의 구성도
<img width="80%" src="https://user-images.githubusercontent.com/102707438/160986663-7994b16c-5c06-48d0-8d95-57834bc82919.png"/>

## 시스템 구성
시스템설명|CPU|메모리|HDD|버전|비고
---|---|---|---|---|---|
RabbitMQ|2|8|20G|3.6.10|VMware
Logstash|3|6|20G|1:7.17.1-1|VMware
Elastic(Master)|2|8|20G|7.17.1|VMware
Elastic(Data1)|i7|16|422G|7.17.1|컴퓨터
Elastic(Data2)|2|6|20G|7.17.1|VMware
HDFS(NameNode)|i5|8|200G|3.2.2|컴퓨터
HDFS(Worker1)|i7|16|550G|3.2.2|컴퓨터
HDFS(Worker2)|i5|16|424G|3.2.2|컴퓨터
HDFS(SecNaNode)|0|0|00G|3.2.2|VMware
Spark(MasterNo)|0|0|00G|3.1.2|컴퓨터
Spark(SlaveNo1)|0|0|00G|3.1.2|컴퓨터
Spark(SlaveNo2)|0|0|00G|3.1.2|컴퓨터

## 프로그램의 설정파일

## 웹 프로젝트

## 분석코드
