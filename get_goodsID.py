import requests
from bs4 import BeautifulSoup
from lxml import etree
import lxml
import pymysql
import csv
import re
# from day_1.聚划算.获取商品ID.create_mysql_table import create_table
# from day_1.聚划算.获取商品ID.create_mysql_table import get_time
from multiprocessing import Pool
import datetime

url = "https://ju.taobao.com/search.htm?words="
base_url = ("https://ju.taobao.com/search.htm?"
            "spm=608.6895169.123.10.4bc32baf5g6WuP&words=&stype=psort&reverse=down&page={page}")
HOST = '10.10.0.104'
USER = "root"
PASSWORD = "5560203@wstSpider!"
PORT = 3306
DB = "taobao_live"
db = pymysql.connect(host=HOST, user=USER, password=PASSWORD, port=PORT, db=DB, charset='utf8')


def create_table():
    cursor = db.cursor()
    sql = "CREATE TABLE if not EXISTS taobao_juhuasuang_all_itemid_{time} LIKE taobao_juhuasuang_all_itemid_template".format(time=get_time())
    cursor.execute(sql)
    cursor.close()
    # db.close()


def get_time():
    today = datetime.date.today()
    return str(today).replace("-", "")



def save_to_mysql(parm):
    cursor = db.cursor()
    sql = "replace into taobao_juhuasuang_all_itemid_{} values(%s,%s,%s,%s,%s)".format(get_time())
    cursor.executemany(sql, parm)
    db.commit()
    cursor.close()


def get_url(page, b_url=base_url):
    return b_url.format(page=page)


def get_html(url):
    # global headers
    headers = {
        "User-agent":("Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36"),
    }
    response = requests.get(url)
    return response.text


def parse_html(html):
    soup = BeautifulSoup(html,'lxml')
    lis = soup.find_all(name='li',class_="item-small-v3")
    for li in lis:
        title = li.find_all(name='h3', class_='nowrap')
        # print(type(title[0]))

        item ={
            "title":  li.find_all(name='h3', class_='nowrap')[0].attrs['title'],
            "url": 'https:' + li.div.a.attrs['href'],
            'price': li.find_all(name='em')[0].text.strip(),
            'sold-num': li.find_all(name='div', attrs={'class': 'sold-num'})[0].em,
            'item_id': re.search('&item_id=(\d*)', li.div.a.attrs['href']).group(1)
        }
        yield item

def parse_xpath(html):
    html = etree.HTML(html)
    lis = html.xpath('//*[@id="content"]/div[1]/ul/li')
    # print(lis[1])
    # //*[@id="content"]/div[1]/ul/li[3]
    # //*[@id="content"]/div[1]/ul/li[1]/div/a/div/div[1]/div[2]/div[2]/div/em
    date_list = []
    for li in lis:

        title = li.xpath('./div/a/h3[@class="nowrap"]/@title')[0],
        url = 'https:' + li.xpath('./div/a/@href')[0],
        price = "".join(li.xpath('.//div[@class="price"]/em//span/text()')),
        item_id = re.search('&item_id=(\d*)', li.xpath('./div/a/@href')[0]).group(1),
        try:
            sold_num = li.xpath('./div/a/div/div[1]/div[2]/div[2]/div/em/text()')[0],
        except:
            sold_num = 0
        parm = (title, url, price, item_id, sold_num)
        date_list.append(parm)
    return date_list


def save_csv(items):
    fieldnames = ['title', 'url', 'sold_num', 'price', 'item_id']
    with open('data.csv', 'w', encoding='utf8',) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow(item)


def get_page_num(url):
    headers = {
        "User-agent": ("Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36"),
    }
    response = requests.get(url, headers=headers)
    # print(response.text)
    # //*[@id="page"]/div[3]/div/div/div/div/div/div[2]/div/span[1]/text()
    num = etree.HTML(response.text).xpath('//*[@id="page"]/div[3]/div/div/div/div/div/div[2]/div/span[1]/text()')
    # print(num)
    num = re.search('\d+', num[0]).group()
    return int(num)


def main(i):
    url = get_url(i)
    # print(url)
    # num = get_page_num(url)
    html = get_html(url)
    data_list = parse_xpath(html)
    # print(data_list)
    print("success")
    save_to_mysql(data_list)
    # save_csv(items)

    if data_list:
        print("第" + str(i) + "页")
    else:
        print("第{}页面内容已为空，".format(i))


def multi_process():
    p = Pool(30)
    beg_url = "https://ju.taobao.com/search.htm?words="
    num = get_page_num(beg_url)
    # print(num)
    num = 120
    for i in range(1, num):
        p.apply_async(main, (i,))
    p.close()
    p.join()




if __name__ == '__main__':
    create_table()
    multi_process()
    db.close()

# def save_mysql():


# html = get_html(url)
# items = parse_xpath(html)
# print(items.__next__())
# # items = parse_html(html)
# # print(items.__next__())
# # save_csv(items)

