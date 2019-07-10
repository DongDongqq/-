# -*- coding: utf-8 -*-
import scrapy
import json
from urllib.parse import urlencode
from scrapy import Spider, Request
from  images360.items import ImageItem

class ImagesSpider(scrapy.Spider):
    name = 'images'
    allowed_domains = ['images.so.com']
    # start_urls = ['http://images.so.com/']

    def start_requests(self):
        data = {'ch': 'beauty', 'listtype': 'new'}
        baseurl = 'https://image.so.com/zjl?'
        for page in range(0, self.settings.get('MAX_PAGE') + 1):
            data["sn"] = page * 30
            params = urlencode(data)
            url = baseurl+params
            yield Request(url, self.parse)

    def parse(self, response):
        result = json.loads(response.text)
        for image in result.get('list'):
            print(image)
            item = ImageItem()
            item["id"] = image.get("id")
            item['url'] = image.get("qhimg_url")
            item['title'] = image.get("title")
            item['thumb'] = image.get("qhimg_thumb")
            yield item




















