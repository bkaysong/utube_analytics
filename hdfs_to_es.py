import os
from functools import reduce
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import json
from itertools import groupby
from pyspark.sql import SparkSession

es = Elasticsearch('http://172.30.1.229:9200', timeout=300)
spark = SparkSession.builder.appName("hdfs_test").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")



f = open("/home/yarn/idx","r")
for i in f.readline():
    file1 = int(i)
    break


#news전처리 # 문자를 단어들로 다 쪼갠 것
r_cm = spark.read.csv("hdfs://172.30.1.61:9000/data/news" + str(file1)+"/*.csv")
df_n = r_cm.select("*").toPandas()
df_n = df_n.rename(columns=df_n.iloc[0])
df_n = df_n.drop(df_n.index[0])
df_news = df_n[['content']]

word_nw = " ".join(i for i in df_news.content.astype(str))
list_word_nw = str.split(word_nw)
mapping = map((lambda x : (x, 1)), list_word_nw)
sorted_mapping = sorted(mapping)
grouper = groupby(sorted_mapping, lambda p:p[0])
news = pd.DataFrame(map(lambda l: (l[0], reduce(lambda x, y: x + y, map(lambda p:p[1], l[1]))), grouper))
js_nw = news.to_json(orient = 'values', force_ascii=False)
docs_nw = json.loads(js_nw)
array = []
for i in range(0,len(docs_nw)):
    js ={
        "word": docs_nw[i][0],
        "count": docs_nw[i][1]
    }
    array.append(js)

helpers.bulk(es, array, index="cold_news_word_test")




#comment전처리 # 문자를 단어들로 다 쪼갠 것

r_cm_w = spark.read.csv("hdfs://172.30.1.61:9000/data/comment" + str(file1)+"/*.csv")
df_m = r_cm_w.select("*").toPandas()
df_m = df_m.rename(columns=df_m.iloc[0])
df_m = df_m.drop(df_m.index[0])
df_cm = df_m[['content']]

word_cm = " ".join(i for i in df_cm.content.astype(str))
list_word_cm = str.split(word_cm)
mapping_cm = map((lambda x : (x, 1)), list_word_cm)
sorted_mapping_cm = sorted(mapping_cm)
grouper_cm = groupby(sorted_mapping_cm, lambda p:p[0])
comment = pd.DataFrame(map(lambda l: (l[0], reduce(lambda x, y: x + y, map(lambda p:p[1], l[1]))), grouper_cm))

js_cm = comment.to_json(orient = 'values', force_ascii=False)

docs_cm = json.loads(js_cm)

array = []
for i in range(0,len(docs_cm)):
    js ={
        "word": docs_cm[i][0],
        "count": docs_cm[i][1]
    }
    array.append(js)

helpers.bulk(es, array, index="cold_comment_word_test")





# cold_comment

r_cm = spark.read.csv("hdfs://172.30.1.61:9000/data/comment" + str(file1)+"/*.csv")
df_c = r_cm.select("*").toPandas()
df_c = df_c.rename(columns=df_c.iloc[0])
df_c = df_c.drop(df_c.index[0])
df_c1 = df_c[['user', 'content', 'likes', 'video_id']]

js_c = df_c1.to_json(orient = 'records')
docs_c = json.loads(js_c)
helpers.bulk(es,docs_c,index="cold_comment_test")
print(docs_c)



#  cold_news



r_cn = spark.read.csv("hdfs://172.30.1.61:9000/data/news" + str(file1)+"/*.csv")
df_cn = r_cn.select("*").toPandas()
df_cn = df_cn.rename(columns=df_cn.iloc[0])
df_cn = df_cn.drop(df_cn.index[0])
df_n1 = df_cn[['date', 'nickname', 'content']]
js_n = df_n1.to_json(orient = 'records')
docs_n = json.loads(js_n)
helpers.bulk(es,docs_n,index="cold_news_test")
print(docs_n)




# cold_video
try:
    r_v = spark.read.csv("hdfs://172.30.1.61:9000/data/video" + str(file1)+"/*.csv")
    # print(df_v)
    df_v = r_v.select("*").toPandas()
    df_v = df_v.rename(columns=df_v.iloc[0])
    df_v = df_v.drop(df_v.index[0])
    df_v1 = df_v[['channel', 'comments', 'crawl_date', 'likes', 'rank', 'subscribers', 'views', 'title', 'video_id']]
    # print(df_v1)
    js_v = df_v1.to_json(orient='records')
    docs_v = json.loads(js_v)
    helpers.bulk(es, docs_v, index="cold_video_test")
    print(docs_v)

except :
    print("csv is null in video")


# 24시간 마다 1씩 idx 파일 내용이 증가함.
file1 += 1
if os.path.exists("/home/yarn/idx"):
    os.remove("/home/yarn/idx")
f = open('/home/yarn/idx', 'w')
f.write(str(file1))




