#!/usr/bin/python3
# -*- coding: utf-8 -*-
from lxml.html import fromstring
import requests
import mysql.connector
import threading

class Book(object):
    #fetch_page: 需要抓取的前几页
    def __init__(self, fetch_page, sql_host, sql_user, sql_password):
        self.fetch_page = fetch_page
        self.fetch_host = 'https://m.39shubao.com'
        self.user_agent = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Mobile Safari/537.36'
        self.sql_host = sql_host
        self.sql_user = sql_user
        self.sql_password = sql_password
    #抓取网站数据
    def __fetchBookTreeData(self, url):
        fetch_url = self.fetch_host + url
        #抓取数据的网站
        print('抓取数据的url---', fetch_url)

        headers = {'User-Agent': self.user_agent}
        response = requests.get(fetch_url, headers=headers)
        #编码
        html = response.content.decode('utf-8')
        #树状数据
        tree = fromstring(html)
        return tree

    #抓取指定页数的数据
    def __fetchPageData(self, page_url):
        #获取书名
        xpath_book_name = '//div[@class="cover"]/p/a[@class="blue"]/text()'
        #获取作者
        xpath_book_author = '//div[@class="cover"]/p/a[contains(@href, "/author/")]/text()'
         #获取详情链接
        xpath_book_detail = '//div[@class="cover"]/p/a[@class="blue"]/@href'

        tree = self.__fetchBookTreeData(page_url)
        names  = tree.xpath(xpath_book_name)
        authors = tree.xpath(xpath_book_author)
        details = tree.xpath(xpath_book_detail)
        return (names, authors, details)

    #抓取详情页数据
    def __fetchBookDetail(self, detail_url):
        #获取连载状态
        xpath_details = '//div[@class="book_box"]/dl/dd/span/text()'
        xpath_newest = '//div[@class="book_box"]/dl/dd/span/a[contains(@href, ".html")]/text()'
        xpath_newest_url = '//div[@class="book_box"]/dl/dd/span/a[contains(@href, ".html")]/@href'

        tree = self.__fetchBookTreeData(detail_url)
        details = tree.xpath(xpath_details)

        status = details[2]
        update = details[3]
        newest = tree.xpath(xpath_newest)[0]
        content_url = tree.xpath(xpath_newest_url)[0]

        return (status, update, newest, content_url)

    #抓取需要保存的页数的数据
    def __fetchNeedData(self, page_num, classify_index):
        print('当前线程----%s' % threading.current_thread().name)

        #分类列表页
        tree = self.__fetchBookTreeData('/list/')

        #获取分类下的链接信息
        href_result = tree.xpath('//div[@class="content"]//ul/li/a/@href')
        #获取分类的id信息
        id_result = tree.xpath('//div[@class="content"]//ul/li/a/@id')
        
        #分类下的列表第一页url
        first_page_url = href_result[classify_index]
        #分类的id
        classify_id = ''
        for id_char in id_result[classify_index]:
            if id_char >= '0' and id_char <= '9':
                classify_id = classify_id + id_char
                
        #页码url  
        page_url = ''

        if page_num == 0:
            #第一页的url和后面的不一样
            page_url = first_page_url
        else:
            #第一页后的url
            page_url = '/sort/%s_%s/' % (classify_id, page_num+1)

        book_data = self.__fetchPageData(page_url)

        for index in range(len(book_data[0])):
            name = book_data[0][index]
            author = book_data[1][index]
            detail_url = book_data[2][index]

            details = self.__fetchBookDetail(detail_url)
            self.__saveFetchData(name, author, details[0], details[1], details[2], details[3])

    #抓取数据入库
    def __saveFetchData(self, name, author, status, update_time, newest, content_url):
        #连接数据库
        connect = mysql.connector.connect(host=self.sql_host, user=self.sql_user, password=self.sql_password, charset='utf8')
        cursor = connect.cursor()

        #创建数据库
        cursor.execute("create database if not exists book character set utf8;")
        #进入数据库
        cursor.execute("use book")
        #创建表
        cursor.execute("""create table if not exists book_list(id int not null auto_increment primary key , 
                        name char(255)character set utf8, 
                        author char(255)character set utf8,
                        status char(255)character set utf8,
                        update_time char(255)character set utf8,
                        newest char(255)character set utf8,
                        content_url char(255)character set utf8);""")

        sql_insert = 'insert into book_list(name, author, status, update_time, newest, content_url) values(%s, %s, %s, %s, %s, %s);'
        sql_select = 'select author from book_list where name=%s;'
        sql_update = 'update book_list set author=%s, status=%s, update_time=%s, newest=%s, content_url=%s where name=%s;'

        #查询对面书是否存在
        cursor.execute(sql_select, [name])
        fet_result = cursor.fetchone()
        if not fet_result:
            #不存在，插入
            cursor.execute(sql_insert, [name, author, status, update_time, newest, content_url])
            print('%s  新书入库' % name)
        else:
            #已存在数据，更新  
            cursor.execute(sql_update, [author, status, update_time, newest, content_url, name])
            print('%s  此书已存在' % name)
        connect.commit()

        cursor.close()
        connect.close()

    #查询数据
    def __searchData(self):
            #连接数据库
        connect = mysql.connector.connect(host=self.sql_host, user=self.sql_user, password=self.sql_password, charset='utf8')
        cursor = connect.cursor()
        #进入数据库
        cursor.execute("use book")
        try:
            cursor.execute('select * from book_list')
            
            print('数据库内书本信息----：\n')
            for book in cursor.fetchall():
                print('书名：%s\n作者：%s\n%s\n%s\n最新章节：%s' % (book[1], book[2], book[3], book[4], book[5]))
        except Exception as error:
            print('出错---', error)

        cursor.close()
        connect.close()

    #删除数据库
    def __deleteData(self):
        #连接数据库
        connect = mysql.connector.connect(host=self.sql_host, user=self.sql_user, password=self.sql_password, charset='utf8')
        cursor = connect.cursor()
        #删除数据库
        try:
            cursor.execute('drop database book')
            print('数据库已删除')
        except Exception as error:
            print('出错---', error)

        cursor.close()
        connect.close()

    #开始抓取数据
    def __startFetchData(self):
        #线程锁
        classifyLock = threading.Lock()
        pageLock = threading.Lock()

        #抓取分类列表页信息
        tree = self.__fetchBookTreeData('/list/')
        result = tree.xpath('//div[@class="content"]//ul/li/a/text()')
        print('分类列表-----', result)

        #循环抓取每个分类
        for fetchClassifyIndex in range(len(result)):
            classifyLock.acquire()
            try:
                for page_num in range(self.fetch_page):
                    #获取锁
                    pageLock.acquire()
                    try:
                        print('开始抓取 %s 分类下 第%s页 的数据' % (result[fetchClassifyIndex], page_num + 1))
                        # fetchNeedData(page, fetchClassifyIndex)
                        book_data_thread = threading.Thread(target=self.__fetchNeedData, args=(page_num, fetchClassifyIndex))
                        book_data_thread.start()
                        book_data_thread.join()
                        print('%s 分类下 第%s页 的数据抓取完毕' % (result[fetchClassifyIndex], page_num + 1))
                    finally:
                        pageLock.release()
            finally:
                classifyLock.release()

    def run(self, is_delete):
        if is_delete == True:
            #删除数据库
            self.__deleteData()
        else:
            # 保存数据
            fetchThread = threading.Thread(target=self.__startFetchData)
            print('开始抓取数据')
            fetchThread.start()
            fetchThread.join()
            print('数据抓取完毕')
        
        #查询数据库数据
        self.__searchData()

#控制删除还是抓取数据 0:删除，否则抓取
book = Book(2, 'localhost', 'root', '12345678')
# book.run(True)
book.run(False)




