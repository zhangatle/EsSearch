from django.db import models
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, connections, Completion

es = connections.create_connection(hosts=["localhost"])  # connection可以连接多台服务器


class Lagou(Document):
    suggest = Completion(analyzer='ik_max_word')
    title = Text(analyzer='ik_max_word')
    url = Keyword()
    url_object_id = Keyword()
    salary = Keyword()
    job_city = Text(analyzer='ik_max_word')
    work_years = Keyword()
    degree_need = Keyword()
    job_type = Text(analyzer='ik_max_word')
    publish_time = Keyword()
    job_advantage = Text(analyzer='ik_max_word')
    job_desc = Text(analyzer='ik_max_word')
    job_address = Text(analyzer='ik_max_word')
    company_url = Keyword()
    company_name = Text(analyzer='ik_max_word')
    crawl_time = Date()
    crawl_update_time = Date()
    tags = Text(analyzer='ik_max_word')

    class Index:
        name = 'lagou'
        settings = {
          "number_of_shards": 2,
          "number_of_replicas": 0
        }


# 知乎提问的elasticsearch的index索引定义
class ZhihuQuestion(Document):
    suggest = Completion(analyzer='ik_max_word')
    title = Text(analyzer='ik_max_word')
    url = Keyword()
    content = Text(analyzer='ik_max_word')
    topics = Text(analyzer='ik_max_word')
    crawl_time = Date()
    answer_num = Integer()
    comments_num = Integer()
    follow_num = Integer()
    view_num = Integer()

    class Index:
        name = 'zhihu_question'
        settings = {
          "number_of_shards": 2,
          "number_of_replicas": 0
        }


# 知乎回答的elasticsearch的index索引定义
class ZhihuAnswer(Document):
    suggest = Completion(analyzer='ik_max_word')
    url = Keyword()
    question_id = Keyword()
    author_id = Keyword()
    content = Text(analyzer='ik_max_word')
    approve_num = Integer()
    comments_num = Integer()
    create_time = Date()
    update_time = Date()
    crawl_time = Date()

    class Index:
        name = 'zhihu_answer'
        settings = {
          "number_of_shards": 2,
          "number_of_replicas": 0
        }