#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import MySQLdb

import scrapy
from scrapy.http import FormRequest
from resume.items import ResumeItem


class BosoSpider(scrapy.Spider):
    name = "boso"
    allowed_domains = ["bosonnlp.com"]
    search_url = 'http://bosonnlp.com/analysis/ner?category=12&sensitivity=3'
    start_urls = ('http://www.bosonnlp.com/', )
    existed_ids = [data['id'] for data in json.loads(open('result.json').read())]

    def get_data(self):
        """从数据库获取数据"""
        conn = MySQLdb.connect(host='localhost',
                               user='root',
                               passwd='haoweiguo1',
                               port=3306,
                               charset='utf8',)
        cur = conn.cursor()
        conn.select_db('resume')
        cur.execute('select id, content from resume')
        one_result = cur.fetchone()
        while one_result:
            if one_result[0] not in self.existed_ids:
                yield one_result
            one_result = cur.fetchone()

    def parse(self, response):
        """入口函数"""
        for data in self.get_data():
            yield FormRequest(self.search_url,
                              formdata={"data": data[1]},
                              callback=self.parse_boso_result,
                              meta={'id': data[0]},
                              dont_filter=True)

    def parse_boso_result(self, response):
        """处理boson返回的结果"""
        result = json.loads(response.body)
        print(result)
        # return
        item = ResumeItem()
        for entity in result[0]['entity']:
            entity_kind = entity[2]
            entity_data = ''.join(result[0]['word'][entity[0]: entity[1]])
            wordline = entity_kind + ':' + entity_data + ','
            if item.get(entity_kind):
                item[entity_kind].append(entity_data)
            else:
                item[entity_kind] = [entity_data]
            print(wordline)
        item['id'] = response.meta.get('id')
        yield item
