"""
    file: DivisionCodeSpider.py
    created time: Tue Oct 15 20:58:43 2019

    民政部行政区划代码增量爬取

    1. 数据库： dcdb
        表：
            fingers
            province
            city
            district

        # 15 双江拉祜族佤族布朗族傣族自治县

        create database dcdb default charset utf8;
        create table fingers(
            finger char(32)
        )charset=utf8;
        use dcdb;

        create table province(
            id int auto_increment,
            name varchar(45),
            code int,
            primary key(id)
        )charset=utf8;

        create table city(
            id int auto_increment,
            name varchar(45),
            code int,
            prov_id int,
            primary key(id),
            foreign key(prov_id) references province(id)
        )charset=utf8;

        create table district(
            id int auto_increment,
            name varchar(45),
            code int,
            city_id int,
            primary key(id),
            foreign key(city_id) references city(id)
        )charset=utf8;
"""
import re
import sys
import time
import pymysql
import requests
from lxml import etree
from hashlib import md5

class DCSpider:
    def __init__(self):
        self.url = 'http://www.mca.gov.cn/article/sj/xzqh/2019/'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
                                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                                     'Chrome/73.0.3683.75 Safari/537.36'}
        self.db = pymysql.connect(host='127.0.0.1', db='dcdb', user='root',
                                  password='123456', port=3306, charset='utf8')
        self.cursor = self.db.cursor()

    def to_encode(self, link):
        m = md5()
        m.update(link.encode())
        return m.hexdigest()

    def get_link(self):
        # 假链接
        html = requests.get(
            url=self.url,
            headers=self.headers
        ).text
        p = etree.HTML(html)
        href_list = p.xpath('//table[@class="article"]/tr[2]/td[2]/a/@href')
        href = href_list[0].strip() if href_list else None
        if not href:
            sys.exit('xpath匹配为空')
        # 真链接
        html = requests.get(
            url='http://www.mca.gov.cn' + href,
            headers=self.headers
        ).text
        pattern = 'window.location.href="(.*?)"'
        r = re.compile(pattern, re.S)
        link_list = r.findall(html)
        link = link_list[0].strip() if link_list else None
        if not link:
            sys.exit('正则匹配为空')

        # 判断是否已经爬取过
        finger = self.to_encode(link)
        sel = 'select finger from fingers where finger=%s'
        rows = self.cursor.execute(sel, finger)
        if rows:
            sys.exit('无更新，不需要增量爬取')
        else:
            return link


    def save_mysql(self):
        # 行政区划代码页面
        link = self.get_link()
        html = requests.get(
            url=link,
            headers=self.headers
        ).text

        p = etree.HTML(html)
        tr_list = p.xpath('//tr[@height=19]')
        prov_id = 0
        city_id = 0
        prev_id = 0
        for tr in tr_list:
            name = tr.xpath('./td[3]/text()')[0].strip()
            code = tr.xpath('./td[2]/text()')[0].strip()

            if code.endswith('0000'):
                # 存入province表中
                data = [name, code]
                ins = 'insert into province(name, code) values(%s, %s)'
                try:
                    self.cursor.execute(ins, data)
                    self.db.commit()
                except Exception as e:
                    print('---mysql error---', e)
                    self.db.rollback()
                sel = 'select id from province where code=%s'
                self.cursor.execute(sel, code)
                prov_id = self.cursor.fetchone()[0]
                prev_id = code
                print('保存省级')

            elif code.endswith('00'):
                # 存入city表中
                data = [name, code, prov_id]
                ins = 'insert into city(name, code, prov_id) values(%s, %s, %s)'
                try:
                    self.cursor.execute(ins, data)
                    self.db.commit()
                except Exception as e:
                    print('---mysql error---', e)
                    self.db.rollback()
                sel = 'select id from city where code=%s'
                self.cursor.execute(sel, code)
                city_id = self.cursor.fetchone()[0]
                prev_id = code
                print('保存市级')

            else:
                if prev_id.endswith('0000'):
                    # 存入city表中
                    data = [name, code, prov_id]
                    ins = 'insert into city(name, code, prov_id) values(%s, %s, %s)'
                    try:
                        self.cursor.execute(ins, data)
                        self.db.commit()
                    except Exception as e:
                        print('---mysql error---', e)
                        self.db.rollback()
                    print('保存直辖市下的区县')
                else:
                    # 存入district表中
                    data = [name, code, city_id]
                    ins = 'insert into district(name, code, city_id) values(%s, %s, %s)'
                    try:
                        self.cursor.execute(ins, data)
                        self.db.commit()
                    except Exception as e:
                        print('---mysql error---', e)
                        self.db.rollback()
                    print('保存区县')


        ins = 'insert into fingers values(%s)'
        finger = self.to_encode(link)
        try:
            self.cursor.execute(ins, finger)
            self.db.commit()
        except Exception as e:
            print('---mysql error---', e)

        self.cursor.close()
        self.db.close()

    def run(self):
        self.save_mysql()

if __name__ == '__main__':
    begin = time.time()
    dc = DCSpider()
    dc.run()
    end = time.time()
    print('共耗时%2.f秒' % (end-begin))




















