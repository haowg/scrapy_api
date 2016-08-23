#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint:disable=relative-import
'''
入口文件
'''

import os
import sys
import time
import logging
from datetime import datetime, timedelta
try:
    import cPickle as pickle
except ImportError:
    import pickle

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from spiders.list_spider import ListSpider
from databases.config import DBSession, MONGO_URI, MONGODB_NAME
from databases.rule import Rule
from test_rule import save_rule


def usage():
    '''
    爬虫使用方法
    '''
    usage_string = 'usage: python %s [save|crawl]\n ' \
           '\n' \
           '\tnull : 使用test_rule中的配置进行一次爬取\n' \
           '\tsave : 保存爬虫规则到数据库\n' \
           '\tcrawl：从爬虫数据库中取得规则并循环爬取 \n ' % sys.argv[0]
    print usage_string
    sys.exit(-1)


def _serialization_rule(rule_object):
    '''
    序列化rule到本地文件‘rule'
    '''
    with open('spiders/rule', 'wb') as rule_file:
        pickle.dump(rule_object, rule_file)


def _load_rule_file():
    '''
    利用rule序列化的文件加载并生成rule类
    '''
    with open('spiders/rule', 'rb') as rule_file:
        cur_rule = pickle.load(rule_file)
    return cur_rule


def _get_default_setting():
    '''
    spider's defautl Settings
    '''
    settings = Settings()
    # crawl settings
    settings.set("DOWNLOADER_MIDDLEWARES", {
        "middlewares.useragentmw.RandomUserAgentMiddleware": 100,
    })
    settings.set("MONGO_URI", MONGO_URI)
    settings.set("MONGODB_NAME", MONGODB_NAME)
    settings.set("DOWNLOAD_DELAY", 1)
    settings.set("DOWNLOAD_TIMEOUT", 8)
    settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", 8)
    settings.set("CONCURRENT_REQUESTS_PER_IP", 3)
    return settings


def _get_spider_process(outputkind):
    '''
    配置并返回爬虫进程
    '''
    # 配置不同的pipleline
    settings = _get_default_setting()
    dict_pipelines = {}
    if outputkind == 'json':
        dict_pipelines['pipelines.JsonWriterPipeline'] = 200
    elif outputkind == 'mongo':
        dict_pipelines['pipelines.MongoPipeline'] = 200
    settings.set("ITEM_PIPELINES", dict_pipelines)
    # add settings
    process = CrawlerProcess(settings)
    return process


def crawl_from_mysql(rules, start_up_time, db):
    '''
    读取配置文件，生成所有到达爬取时间的爬虫，并执行
    '''
    for rule in rules:
        # 已经到达下次执行时间的爬虫规则
        if rule.next_crawl_time <= datetime.now():
            new_time = datetime.now() + timedelta(minutes=rule.interval_time)
            db.query(Rule).filter(Rule.id == rule.id).update({'next_crawl_time':new_time})
            db.commit()
            # 解决slqalchemy的懒加载问题
            logging.log(logging.WARNING, 'crawling %s now', rule.name)
            # 序列化rule类
            _serialization_rule(rule)
            # 执行爬虫
            os.system('python run.py exec')
        # 更新下次启动的时间
        if rule.next_crawl_time < start_up_time:
            start_up_time = rule.next_crawl_time
    return start_up_time


def crawlers_start():
    '''
    从数据库读取配置，生成爬虫，爬取信息
    '''
    start_up_time = datetime.now()
    while 1:
        logging.log(logging.WARNING, 'start_up_time : %s', start_up_time)
        # 到达启动时间？启动:暂停一段时间
        if start_up_time <= datetime.now():
            start_up_time += timedelta(days=1)
            # 获取获取数据中存储的rules
            db = DBSession()
            rules = db.query(Rule).filter(Rule.enable == 1)
            # 从数据库的配置生成爬虫并执行
            start_up_time = crawl_from_mysql(rules, start_up_time, db)
            db.close()
        else:
            sleeping_time = start_up_time - datetime.now()
            logging.log(logging.WARNING, 'sleep time %s', sleeping_time)
            time.sleep(sleeping_time.total_seconds())


def crawl(output_type, process_links):
    '''
    执行爬虫
    '''
    # 加载rule
    spider_rule = _load_rule_file()
    process = _get_spider_process(output_type)
    # 配置爬虫
    process.crawl(ListSpider, spider_rule, process_links=process_links)
    process.start()


if __name__ == '__main__':
    # 测试模式
    if len(sys.argv) < 2:
        from test_rule import rule
        _serialization_rule(rule)
        crawl('json', None)
    # 执行模式
    elif sys.argv[1] == 'exec':
        crawl('mongo', 'filter_links')
    # 爬取模式
    elif sys.argv[1] == 'crawl':
        crawlers_start()
    # 保存模式
    elif sys.argv[1] == 'save':
        try:
            save_rule()
        except Exception, e:
            print '保存失败'
            sys.exit(-1)
        print '保存成功'
    # 参数错误
    else:
        usage()

