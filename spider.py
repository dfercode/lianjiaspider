#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
# Author 李浩杰;
import json
from multiprocessing import Pool
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
import pandas as pd
import pymongo

# 定义全局变量从城市名到城市名简写的映射，用于指定城市名爬取该城市的记录
dicCity = {'大连': 'dl', '北京': 'bj', '广州': 'gz', '沈阳': 'sy', '佛山': 'fs',
           '天津': 'tj', '成都': 'cd', '南京': 'nj', '杭州': 'hz', '青岛': 'qd',
           '厦门': 'xm', '武汉': 'wh', '重庆': 'cq', '长沙': 'cs', '济南': 'jn',
           '东莞': 'dg', '西安': 'xa', '石家庄': 'sjz', '珠海': 'zh', '郑州': 'zz',
           '洛阳': 'ly', '哈尔滨': 'hrb'}

ua = UserAgent()
headers1 = {'User-Agent': ua.random}

Mongo_Url = 'localhost'
Mongo_DB = 'LianjiaDB'
Mongo_TABLE = '北京' #在这里输入想要爬取的城市
getpageNum=5        #在这里输入想要爬取的页数
client = pymongo.MongoClient(Mongo_Url)
db = client[Mongo_DB]

#-------------------------------------------------------
#爬取网页房源数据
#-------------------------------------------------------
# 生成url
def generate_allurl(user_in_nub, user_in_city):
    url = 'http://' + user_in_city + '.lianjia.com/ershoufang/pg{}/'
    for url_next in range(1, int(user_in_nub)):
        yield url.format(url_next)

#分析url解析出每一页的详细url
def get_allurl(generate_allurl):  #
    get_url = requests.get(generate_allurl, 'lxml', headers=headers1)
    if get_url.status_code == 200:
        print(get_url)
        re_set = re.compile('<a class="img".*?href="(.*?)"')
        re_get = re.findall(re_set, get_url.text)
        re_get.pop()
        return re_get

#解析网站，提取网站信息
def open_url(re_get):  # 分析详细url获取所需信息
    res = requests.get(re_get, 'lxml', headers=headers1)
    if res.status_code == 200:
        info = {}
        # 获取房源基本属性
        soup = BeautifulSoup(res.text, 'html.parser')
        info['标题'] = soup.select('.main')[0].text
        print(info['标题'])
        info['总价'] = soup.select('.total')[0].text + '万'
        info['每平方售价'] = soup.select('.unitPriceValue')[0].text
        info['参考总价'] = soup.select('.taxtext')[0].text
        info['建造时间'] = soup.select('.subInfo')[2].text
        info['小区名称'] = soup.select('.info')[0].text
        info['所在区域'] = soup.select('.info a')[0].text + ':' + soup.select('.info a')[1].text
        info['链家编号'] = str(re_get)[33:].rsplit('.html')[0]
        for i in soup.select('.base li'):
            i = str(i)
            if '</span>' in i or len(i) > 0:
                key, value = (i.split('</span>'))
                info[key[24:]] = value.rsplit('</li>')[0]
        # 获取房源交易属性
        for i in soup.select('.transaction li'):
            i = str(i)
            if '</span>' in i and len(i) > 0 and '抵押信息' not in i:
                i = i.replace('\n', '')
                # temp=
                key, value = (i.split('</span>')[0:2])
                info[key[24:]] = value.rsplit('<span>')[1]
        # 获取房源特色
        listName=soup.select('div[class="baseattribute clear"] div[class="name"]')
        listContent=soup.select('div[class="baseattribute clear"] div[class="content"]')
        for i in range(len(listName)) :
            listName[i] = str(listName[i])[18:-6]
            listContent[i]=str(listContent[i]).split('\n')[1][20:]
            info[listName[i]]=listContent[i]
        updateToMongoDB(info)
        pandas_to_xlsx(info,Mongo_TABLE+'.csv')
        return info.values()

# 将爬取的信息添加到数据库
def updateToMongoDB(datas):
    col=Mongo_TABLE
    db[col].update({'链家编号':datas['链家编号']},{'$set':datas},True)



#将爬取数据存储到CSV文件
def pandas_to_xlsx(info, fileName):  # 储存到CSV
    list1=info.keys()
    pd_look = pd.DataFrame([list1,info.values()])
    pd_look.to_csv( fileName, encoding='gbk', mode='a'
            )

#将爬取数据存储到txt文件
def writer_to_text(list):  # 储存到text
    with open('1.txt', 'a', encoding='utf-8')as f:
        f.write(json.dumps(list, ensure_ascii=False) + '\n')
        f.close()

#并行执行的进程函数，用于多进程实现并行爬取网站数据
def main(url):
    open_list = open_url(url)
    return list(open_list)


#   多进程实现读取指定城市的指定页数的房源数据
def getDataFromWeb(city, pagenum, outCsvFileName):
    pool = Pool(10)
    fileName = outCsvFileName
    if len(outCsvFileName) == 0:
        fileName = city + '.csv'
    for i in generate_allurl(pagenum, city):
        print(i)
        allurl = get_allurl(i)
        # for j in range( len(allurl)):
        #     a=main(allurl[j])
        a = pool.map(main, allurl)
        print(a)

        #pandas_to_xlsx(a, fileName)

#-------------------------------------------------------#
# 用MongoDB实现查询功能
#-------------------------------------------------------#
# 根据面积范围查询 面积(maxArea,minArea)
def selectOrderArea(minArea=None,maxArea=None,source=None):
    list1 =[]
    aaa=db[Mongo_TABLE].find()
    if source!=None:
        for i in source:
            str1 = str(i['建筑面积'])
            str1 = str1.replace('㎡', '')
            str1 = float(str1)
            if minArea == None and maxArea == None:
                return None
            elif minArea != None and str1 > minArea and maxArea==None:
                list1.append(i)
            elif maxArea != None and str1 < maxArea and minArea==None:
                list1.append(i)
            elif str1 > minArea and str1 < maxArea:
                print(i)
                list1.append(i)
        return list1
    for i in db[Mongo_TABLE].find():
        str1=str(i['建筑面积'])
        str1=str1.replace('㎡','')
        str1=float(str1)
        if minArea==None and maxArea==None:
            return source
        elif minArea!=None and str1>minArea and maxArea==None:
            list1.append(i)
        elif maxArea!=None and str1<maxArea and minArea==None:
            list1.append(i)
        elif str1>minArea and str1<maxArea:
            #print(i)
            list1.append(i)
    return list1
    #db[Mongo_TABLE].find({},{'建筑面积':{'$lt':maxArea,'$gt':minArea}})

# 根据每平方价格查询 单价(maxUnitPrice,minUnitPrice)
def selectOrderUnitPrice(minUnitPrice,maxUnitPrice,source=None):
    list1=[]
    if source!=None:
        for i in source:
            str1 = str(i['每平方售价'])
            str1 = str1.replace('元/平米', '')
            str1 = float(str1)
            if minUnitPrice == None and maxUnitPrice == None:
                return None
            elif minUnitPrice != None and str1 > minUnitPrice and maxUnitPrice==None:
                list1.append(i)
            elif minUnitPrice != None and str1 < maxUnitPrice and minUnitPrice==None:
                list1.append(i)
            elif str1 > minUnitPrice and str1 < maxUnitPrice:
                # print(i)
                list1.append(i)
        return list1
    for i in db[Mongo_TABLE].find():
        str1=str(i['每平方售价'])
        str1=str1.replace('元/平米','')
        str1=float(str1)
        if minUnitPrice==None and maxUnitPrice==None :
            return source
        elif minUnitPrice!=None and str1>minUnitPrice and maxUnitPrice==None :
            list1.append(i)
        elif minUnitPrice!=None and str1<maxUnitPrice and minUnitPrice==None:
            list1.append(i)
        elif str1>minUnitPrice and str1<maxUnitPrice:
            #print(i)
            list1.append(i)
    return list1
# 根据区域查询 region
def selectOrderRegion(region,source=None):
    res = []
    if region==None:
        return source
    if source!= None:
        for i in source:
            if i["所在区域"].find(region)!=-1:
                res.append(i)
        return res
    res=db[Mongo_TABLE].find({ "所在区域":{'$regex':region}})
    return res

    #for i in res:
        #print(i)
# 根据小区名称查询 neighbourhood
def selectOrderNeighbourhood(neighbourhood,source=None):
    res=[]
    if neighbourhood==None:
        return source
    if source!= None:
        for i in source:
            if i["小区名称"].find(neighbourhood)!=-1:
                res.append(i)
        return res
    res = db[Mongo_TABLE].find({"小区名称": {'$regex': neighbourhood}})
    return res
    #for i in res:
        #print(i)
#综合查询
def selectComprehensive(minArea=None,maxArea=None,minUnitPrice=None,
                        maxUnitPrice=None,region=None,neighbourhood=None):
    r=db[Mongo_TABLE].find()
    res=selectOrderArea(minArea,maxArea)
    
    if len(res) <1:
        return None
    res = selectOrderUnitPrice(minUnitPrice,maxUnitPrice,res)
    if len(res) <1:
        return None
    res = selectOrderRegion(region,  res)
    if len(res) <1:
        return None
    res = selectOrderNeighbourhood(neighbourhood, res)
    if len(res) <1:
        return None
    return res
if __name__ == '__main__':
    # user_in_city = input('输入爬取城市：')
    # user_in_nub = input('输入爬取页数：')
    #查看数据库

    city = Mongo_TABLE #要爬取的城市


    pageNum = getpageNum            #要爬取的页数
    outCsvName = city + '.csv'#爬取的到的csv文件

    #开始爬取指定给城市指定页码数的数据
    getDataFromWeb(dicCity[city], pageNum, outCsvName,)


    print('根据面积范围查询_____________________________________')
    #根据面积范围查询 面积(maxArea,minArea)
    minArea = 40
    maxArea = 100
    res = selectOrderArea(minArea, maxArea)
    for i in res :
        print(i)

    print('每平方价格查询_____________________________________')
    #根据每平方价格查询 单价(maxUnitPrice,minUnitPrice)
    maxUnitPrice=10000
    minUnitPrice=5000
    res = selectOrderUnitPrice(minUnitPrice,maxUnitPrice )
    for i in res :
        print(i)

    print('根据区域查询_____________________________________')
    #根据区域查询 region
    region='浑南'
    res = selectOrderRegion(region)
    for i in res :
        print(i)

    print('根据小区名称查询_____________________________________')
    #根据小区名称查询 neighbourhood
    neighbourhood='万科新里程'
    res = selectOrderNeighbourhood(neighbourhood)
    for i in res :
        print(i)

    print('综合查询_____________________________________')
    #  综合查询
    neighbourhood='东亚国际城'
    res = selectComprehensive(minArea,maxArea,minUnitPrice,maxUnitPrice
                              ,region,neighbourhood)
    if res !=None:
        for i in res :
            print(i)

