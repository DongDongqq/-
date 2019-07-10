import requests
import pymysql
import time
import re
import json
import multiprocessing
from multiprocessing import Pool
# from day_1.聚划算.获取商品详情页.create_mysql_table import create_table
# from day_1.聚划算.获取商品详情页.create_mysql_table import get_time
import datetime

"""
    1、读取数据库id
    2、设置时间戳，毫秒
    3、拼接url，请求
    4、解析
"""

class JhsSpider(object):

    HOST = '10.10.0.104'
    USER = 'root'
    PASSWORD = "5560203@wstSpider!"
    PORT = 3306
    DB = "taobao_live"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N)"
                       " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Mobile Safari/537.36"),
        "Referer": ("https://ju.taobao.com/m/jusp/alone/detailwap/mtp.htm?"
                    "item_id=575784921469&ju_id=10000122764391&spm=608.6895169.456.1.e6ad2bafFxgfZr&id=10000122764391&&")
    }
    db_ = pymysql.connect(host=HOST, user=USER, password=PASSWORD, db=DB, port=PORT, charset='utf8')


    def __init__(self):
        self.base_url = ('https://detail.ju.taobao.com/detail/json/mobile_dynamic.do?'
                    'item_id={item_id}&groupId=&sales_site=1&pinExtra=&_={new_time}&callback=jsonp1')
        self.item_ids = None
        self.url_iterator = None
        # self.db_ = JhsSpider.connect_to_mysql()

    @classmethod
    def connect_to_mysql(cls):
        db = pymysql.connect(host=cls.HOST, user=cls.USER, password=cls.PASSWORD, db=cls.DB, port=cls.PORT)
        return db

    @classmethod
    def close_db_(cls):
        cls.db_.close()

    def read_from_mysql(self):
        cursor = self.db_.cursor()
        # sql = "select itemId from taobao_juhuasuang_all_itemid_{time}".format(time=get_time())
        sql = ("SELECT a.itemId from taobao_juhuasuang_all_itemid_{time_1} a left join "
               "(SELECT itemId from taobao_juhuasuang_all_details_items_{time_2}) as b on  "
               "a.itemId=b.itemId where b.itemId is null").\
            format(time_1=get_time(), time_2=get_time())
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            self.item_ids = result
        except Exception as e:
            print(e.args)
            cursor.close()
            self.db_.close()

    def get_time(self):
        new_time = int(round(time.time() * 1000))
        return new_time

    def get_urls(self):
        self.read_from_mysql()
        # print(self.item_ids)
        urls_iterator = map(self.create_url, self.item_ids)
        self.url_iterator = urls_iterator
        # print(self.url_iterator)

    def create_url(self, id_):
        # print(id_[0])
        new_url = self.base_url.format(item_id=id_[0], new_time=self.get_time())
        return new_url

    @classmethod
    def get_json(cls, url):
        try:
            response = requests.get(url, headers=cls.headers,timeout=10)
            html=response.text
            re_json = re.findall('jsonp1\((.*)\)', html, re.S)[0]
            json_ = json.loads(re_json)
            # print("text_1")
            return json_
        except Exception as e:
            print(e)
            print("url:" + url)
            return None

    @classmethod
    def parse_json(cls, json):
        if json:
            childCategory = json.get('item').get('childCategory')
            picUrlNew = json.get('item').get("picUrlNew")
            soldCount = json.get('item').get('soldCount')
            activityPrice = json.get('item').get('activityPrice') / 100
            originalPrice = json.get('item').get('originalPrice') / 100
            sellerId = json.get('item').get('sellerId')
            onlineEndTime = json.get('item').get('onlineEndTime')
            # onlineEndTime = str(onlineEndTime)
            formatEndTime = cls.format_time(onlineEndTime)
            onlineStartTime = json.get('item').get('onlineStartTime')

            formatStartTime = cls.format_time(onlineStartTime)
            itemId = json.get('item').get('itemId')
            channelId = json.get('item').get('channelId')
            sellerNick = json.get('item').get('sellerNick')
            remindCount = json.get('item').get('remindCount')
            descframeURL = "https:" + json.get('item').get('descframeURL')
            tuple_1 = (childCategory, picUrlNew, soldCount, activityPrice, originalPrice, sellerId, onlineEndTime,
                       formatEndTime, onlineStartTime, formatStartTime, itemId,
                       channelId, sellerNick, remindCount, descframeURL)
            return tuple_1
        else:
            print("json == None")
            return None

    @classmethod
    def toString(cls, tuple_):
        print(tuple_)

    @classmethod
    def format_time(cls, time_num):
        time_stru = time.localtime((time_num / 1000))
        format_time = time.strftime('%Y-%m-%d %H:%M:%S', time_stru)
        return format_time

    @classmethod
    def save_to_mysql(cls, itmes):
        sql = "replace into taobao_juhuasuang_all_details_items_{time} values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(time=get_time())
        cursor = cls.db_.cursor()
        cursor.executemany(sql, itmes)
        cls.db_.commit()
        cursor.close()

    def many_spider(self):
        self.get_urls()
        n = 100
        url_lists = list(self.url_iterator)
        urls_list = [url_lists[i: i + n] for i in range(0, len(url_lists), n)]
        # print(urls_list)
        p_ = Pool(30)
        for urls in urls_list:
            # print("duan_2")
            p_.apply_async(JhsSpider.main, (urls,))
            # print("duan_4")
            # self.main(urls)
        p_.close()
        p_.join()

    def single_text(self):
        self.get_urls()
        url_1 = list(self.url_iterator)[0]
        print(url_1)
        self.main(url_1)

    @classmethod
    def main(cls, urls):
        # print("duan_3")
        # print(urls)
        datas = []
        for url in urls:
            json_ = cls.get_json(url)
            tuple_ = cls.parse_json(json_)
            # cls.toString(tuple_)
            if tuple_:
                datas.append(tuple_)
                print("success")
        # cls.toString(datas)

        print("=" * 50)
        cls.save_to_mysql(datas)

    def process_start(list_pingjun):
        process = []
        # num_cpus = multiprocessing.cpu_count()
        # print('将会启动进程数为：', num_cpus)
        for i in list_pingjun:
            print('启动进程'.center(100, '='))
            # print(i)
            p = multiprocessing.Process(target=main, args=(i,))  ##创建进程
            p.start()  ##启动进程
            process.append(p)  ##添加进进程队列
        for p in process:
            print('end进程'.center(100, '='))
            # p.join(timeout=300)  ##等待进程队列里面的进程结束


def create_table():
    cursor = JhsSpider.db_.cursor()
    sql = "CREATE TABLE if not EXISTS taobao_juhuasuang_all_details_items_{time} LIKE taobao_juhuasuang_all_details_items_template".format(time=get_time())
    cursor.execute(sql)
    cursor.close()


def get_time():
    today = datetime.date.today()
    return str(today).replace("-", "")


if __name__ == '__main__':
    create_table()
    spider = JhsSpider()
    print("begin")
    spider.many_spider()

    print("end")
    spider.db_.close()

