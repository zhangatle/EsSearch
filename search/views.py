import json
import pickle

from django.http import HttpResponse
from django.views.generic.base import View
from search.models import Lagou
import redis
from django.shortcuts import render
from django.utils.datastructures import OrderedSet
from datetime import datetime
from elasticsearch import Elasticsearch
from django.views.generic.base import RedirectView

redis_cli = redis.Redis(host='localhost', port=6379, db=0, password='Itzler.666')
client = Elasticsearch(hosts=['localhost'])


class IndexView(View):
    @staticmethod
    def get(request):
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5
        )
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    @staticmethod
    def get(request):
        keywords = request.GET.get('s', '')
        current_type = request.GET.get('s_type', '')
        if current_type == 'question':
            return_suggest_list = []
            if keywords:
                s = Lagou.search()
                s = s.suggest("my_suggest", keywords, completion={
                    "field": "suggest",
                    "fuzzy": {
                        "fuzziness": 20
                    },
                    "size": 10
                })
                suggestions = s.execute()
                for match in suggestions.suggest.my_suggest[0].options[:10]:
                    source = match._source
                    return_suggest_list.append(source["title"])
            return HttpResponse(json.dumps(return_suggest_list), content_type="application/json")
        elif current_type == 'job':
            return_suggest_list = []
            if keywords:
                s = Lagou.search()
                s = s.suggest("my_suggest", keywords, completion={
                    "field": "suggest",
                    "fuzzy": {
                        "fuzziness": 20
                    },
                    "size": 10
                })
                suggestions = s.execute()
                name_set = OrderedSet()
                for match in suggestions.suggest.my_suggest[0].options[:10]:
                    source = match._source
                    name_set.add(source["title"])
                for name in name_set:
                    return_suggest_list.append(name)
            return HttpResponse(json.dumps(return_suggest_list), content_type="application/json")


class SearchView(View):
    @staticmethod
    def get(request):
        key_words = request.GET.get('q', '')
        # 实现关键词keyword加1操作
        redis_cli.zincrby("search_keywords_set", 1, key_words)
        # 获取topn个搜索词
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5
        )
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean

        job_count = redis_cli.get("lagou_job_count")
        if job_count:
            job_count = pickle.loads(job_count)
        else:
            job_count = 0

        zhihu_question_count = redis_cli.get("zhihu_question_count")
        zhihu_answer_count = redis_cli.get("zhihu_answer_kcount")
        if zhihu_question_count:
            zhihu_question_count = pickle.loads(zhihu_question_count)
        else:
            zhihu_question_count = 0

        if zhihu_answer_count:
            zhihu_answer_count = pickle.loads(zhihu_answer_count)
        else:
            zhihu_answer_count = 0

        zhihu_count = zhihu_answer_count + zhihu_question_count

        # 当前要获取第几页的数据
        page = request.GET.get("p", "1")
        try:
            page = int(page)
        except BaseException:
            page = 1
        response = []
        start_time = datetime.now()
        s_type = request.GET.get("s_type", "")
        if s_type == "job":
            response = client.search(
                index="lagou",
                request_timeout=60,
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": [
                                "title",
                                "tags",
                                "job_desc",
                                "job_advantage",
                                "company_name",
                                "job_address",
                                "job_city",
                                "degree_need"
                            ]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyword">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "job_desc": {},
                            "company_name": {}
                        }
                    }
                }
            )
        end_time = datetime.now()
        last_seconds = (end_time - start_time).total_seconds()

        hit_list = []
        error_nums = 0
        if s_type == 'job':
            for hit in response['hits']['hits']:
                hit_dict = {}
                try:
                    if "title" in hit['highlight']:
                        hit_dict['title'] = "".join(hit['highlight']['title'])
                    else:
                        hit_dict['title'] = hit['_source']['title']
                    if "job_desc" in hit['highlight']:
                        hit_dict['content'] = "".join(hit["highlight"]["job_desc"][:150])
                    else:
                        hit_dict['content'] = hit['_source']['job_desc'][:150]
                    hit_dict['create_date'] = hit['_source']['publish_time']
                    hit_dict['url'] = hit['_score']
                    hit_dict['score'] = hit['_source']['publish_time']
                    hit_dict['company_name'] = hit['_source']['company_name']
                    hit_dict['source_site'] = "拉勾网"
                    hit_list.append(hit_dict)
                except:
                    hit_dict['title'] = hit['_source']['title']
                    hit_dict['job_desc'] = hit['_source']['job_desc']
                    hit_dict['create_date'] = hit['_source']['publish_time']
                    hit_dict['url'] = hit['_score']
                    hit_dict['score'] = hit['_source']['publish_time']
                    hit_dict['company_name'] = hit['_source']['company_name']
                    hit_dict['source_site'] = "拉勾网"
                    hit_list.append(hit_dict)
        total_nums = int(response['hits']['total']['value'])

        # 计算总页数
        if(page % 10) > 0:
            page_nums = int(total_nums / 10) + 1
        else:
            page_nums = int(total_nums / 10)

        return render(request, "result.html", {
            "page": page,
            "all_hits": hit_list,
            "key_words": key_words,
            "total_nums": total_nums,
            "page_nums": page_nums,
            "last_seconds": last_seconds,
            "topn_search": topn_search,
            "s_type": s_type,
            "job_count": job_count,
            "zhihu_count": zhihu_count
        })


favicon_view = RedirectView.as_view(
    url="http://localhost:8000/favicon.ico",
    permanent=True
)


