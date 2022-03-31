import pandas
from elasticsearch import Elasticsearch
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
import os
from datetime import datetime, timedelta

# pandas dataframe to spark dataframe 사용자 라이브러리
sqlContext = SQLContext(SparkContext.getOrCreate())
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# 실행시마다 증가하는 변수를 만들려 한다. 파일에 숫자를 저장하는 방식으로 구현했다.
# 처음에 idx라는 파일에는 1이라는 숫자가 저장되어있다. file1이라는 변수에 1이 들어간다.
# 실행할 때 마다 값이 1씩 증가한다.
f = open("/home/yarn/idx","r")
for i in f.readline():
    file1 = int(i)
    break

# 현재로부터 하루 전의 시간을 계산하여 time_before_one_day에 저장한다.
now = datetime.now()
before_one_day = now - timedelta(days=1)
before_one_day = str(before_one_day)
before_one_day = before_one_day.replace(" ", "T")
time_before_one_day = before_one_day.strip().split(".")[0]
time_before_one_day = time_before_one_day + "+09:00" # UTC 서울 표준 표기법은 2022-03-30T09:10:05+09:00 형식이다.

es = Elasticsearch('http://172.30.1.229:9200', timeout=1000)

# total num of Elasticsearch documents to get with API call
total_docs = 300000

# query
# lte = less than or equal
q ={
    "query":{
        "range":{
            "@timestamp":{ "lte" : time_before_one_day }
        }
    }
}
# elastic indexes
indexes = ["news", "video", "comment"]

# 각각의 인덱스에 접근, 현재로부터 24시간 이전의 데이터를 조회
for index in indexes:
    print(index, "index to hdfs Start")
    response = es.search(
        index=index,
        body=q,
        size=total_docs
    )

    # grab list of docs from nested dictionary response
    es_docs = response["hits"]["hits"]

    #  create an empty Pandas DataFrame object for docs
    docs = pandas.DataFrame()

    # iterate each Elasticsearch doc in list
    for num, doc in enumerate(es_docs):

        # get _source data dict from document
        source_data = doc["_source"]

        # get _id from document
        _id = doc["_id"]

        # create a Series object from doc dict object
        doc_data = pandas.Series(source_data, name = _id)

        # append the Series object to the DataFrame object
        docs = docs.append(doc_data)
    # transform type double to integer
    try:
        if index == 'video':
            docs = docs.astype({'views': 'int64', 'rank': 'int64', 'comments': 'int64', 'likes': 'int64', 'subscribers': 'int64', '@version': 'int64'})
        elif index == 'comment':
            docs = docs.astype({'likes': 'int64', '@version': 'int64'})
    except:
        print("there is no document in 24 hours in index :", index)
        continue

    # export Elasticsearch documents to a CSV file
    docs.to_csv("/home/yarn/" + index + ".csv", ",")  # CSV delimited by commas

    # 현재로부터 24시간 이전의 데이터를 엘라스틱에서 제거
    es.delete_by_query(index=index, body=q)

    # start spark
    findspark.init()
    spark = SparkSession.builder.getOrCreate()

    # pandas data frame to spark data frame
    data = pandas.read_csv("/home/yarn/" + index + ".csv")
    df = pandas_to_spark(data)

    # save to hdfs
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(
        "hdfs://172.30.1.61:9000/data/" + index + str(file1))

    # local computer에 생성된 csv 파일 제거
    file_path = "/home/yarn/" + index + ".csv"
    if os.path.exists(file_path):
        os.remove(file_path)

    print(index, "index to hdfs Finish")