# -*- coding: utf-8 -*-

import scrapy


class ResumeItem(scrapy.Item):
    time = scrapy.Field()
    org_name = scrapy.Field()
    job_title = scrapy.Field()
    location = scrapy.Field()
    product_name = scrapy.Field()
    company_name = scrapy.Field()
    person_name = scrapy.Field()
    id = scrapy.Field()
