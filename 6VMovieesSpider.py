#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
import time
import sqlite3
import requests
import threading
from bs4 import BeautifulSoup


def mythread(TitleInfos,n):
    Parserhtmlstr = geturltext(TitleInfos[n][1])
    regex_dotall = re.compile('classid=(\d+)&id=(\d+)"', re.S)
    ClassIDs = regex_dotall.findall(Parserhtmlstr)
    classid = ClassIDs[0][0]
    id = ClassIDs[0][1]
    CommentsUrl = 'http://www.hao6v.com/e/pl/?classid=' + classid + '&id=' + id
    sourceurl = "http://www.hao6v.com/e/public/ViewClick?classid=" + classid + "&id=" + id + "&down=3"
    moviedatadic = getmoviedata(Parserhtmlstr)
    Source = GetSours(sourceurl)
    commentcount = getcommentsinfo(CommentsUrl)[0]
    movienamelist = getmoviename(TitleInfos[n][2])
    if movienamelist[0]:
        moviename = movienamelist[1]
    else:
        moviename = movienamelist[1]
    datebaselist = []
    datebaselist.append(TitleInfos[n][1])
    tempstr = TitleInfos[n][2].replace("<font color='#FF0000'>", "")
    tempstr = tempstr.replace("</font>", "")
    datebaselist.append(tempstr.strip().replace("'", ""))
    datebaselist.append(TitleInfos[n][0])
    datebaselist.append(moviename.strip().replace("'", ""))
    datebaselist.append(Source)
    datebaselist.append(commentcount)
    tempstr = str(moviedatadic["译名"].replace("'", ""))
    Translationname = tempstr.strip()
    datebaselist.append(Translationname)
    datebaselist.append(moviedatadic["片名"].strip().replace("'", ""))
    datebaselist.append(moviedatadic["年代"].strip().replace("'", ""))
    datebaselist.append(moviedatadic["产地"].strip().replace("'", ""))
    datebaselist.append(moviedatadic["类别"].strip().replace("'", ""))
    datebaselist.append(moviedatadic["语言"].strip().replace("'", ""))
    datebaselist.append(moviedatadic["字幕"].strip().replace("'", ""))
    datebaselist.append(moviedatadic["上映日期"].strip().replace("'", ""))
    tempstrlist = getscorecount(moviedatadic["IMDB评分"])
    datebaselist.append(tempstrlist[0])#IMDB评分
    datebaselist.append(tempstrlist[1])#IMDB评分人数
    tempstrlist = getscorecount(moviedatadic["豆瓣评分"])
    datebaselist.append(tempstrlist[0])  # 豆瓣评分
    datebaselist.append(tempstrlist[1])  # 豆瓣评分人数
    datebaselist.append(moviedatadic["集数"].strip())
    datebaselist.append(moviedatadic["片长"].strip().replace("'", ""))
    datebaselist.append((moviedatadic["导演"].strip().replace("'", "")))
    datebaselist.append((moviedatadic["编剧"].strip().replace("'", "")))
    datebaselist.append((moviedatadic["主演"].strip().replace("'", "")))
    datebaselist.append((moviedatadic["简介"].strip().replace("'", "")))
    datebaselist.append(moviedatadic["获奖情况"].strip().replace("'", ""))
    sqlstr = 'insert into test (movie_url,title,uploaddate,moviename,score_6v,scorecount_6v,Translation,filmname,year,country,type,language,subtitles,release_date,score_imdb,scorecount_imdb,score_douban,scorecount_douban,TVcount,Runtime,director,Writer,Stars,stotyline,awards) values (\'{0}\',\'{1}\',\'{2}\',\'{3}\',\'{4}\',\'{5}\',\'{6}\',\'{7}\',\'{8}\',\'{9}\',\'{10}\',\'{11}\',\'{12}\',\'{13}\',\'{14}\',\'{15}\',\'{16}\',\'{17}\',\'{18}\',\'{19}\',\'{20}\',\'{21}\',\'{22}\',\'{23}\',\'{24}\');'.format(
        datebaselist[0], datebaselist[1], datebaselist[2], datebaselist[3], datebaselist[4], datebaselist[5],
        datebaselist[6], datebaselist[7], datebaselist[8], datebaselist[9], datebaselist[10], datebaselist[11],
        datebaselist[12], datebaselist[13], datebaselist[14], datebaselist[15], datebaselist[16], datebaselist[17],
        datebaselist[18], datebaselist[19], datebaselist[20], datebaselist[21], datebaselist[22], datebaselist[23],datebaselist[24])
    writerdatabase(moviename,sqlstr)
    # print("大量写入")
    # sqlstr2 = "UPDATE moviesdata SET Translation = '{0}' WHERE movie_url = '{1}';".format(Translationname,TitleInfos[n][1])
    # print(sqlstr2)
    # sqlstr = sqlstr1+sqlstr2
    # writerdatabase(moviename, sqlstr2)
    # print("一个量写入")
def main(PageNum,PageMoviesNum):
    conn = sqlite3.connect('E:\SQLite\SQLiteData\movies6v.db')
    cursor = conn.cursor()
    sqlstr = "select movie_url from test"
    cursor.execute(sqlstr)
    conn.commit()
    values = cursor.fetchall()
    databasedic = {}
    for i in values:
        databasedic[i[0]] = 0
    cursor.close()
    conn.close()
    for i in range(int(PageNum)):
        print("正在获取第{0}页的电影信息...".format(int(i)+1))
        if i == 0:
            Url = HomeUrl
            pass
        else:
            Url = HomeUrl + "index_" + str(int(i)+1) + ".html"
        htmlstr = geturltext(Url)
        regex_dotall = re.compile('<li><span>(.*?)</span><a href="(.*?)" target="_blank">(.*?)</a></li>',re.S)
        TitleInfos = regex_dotall.findall(htmlstr)

        # 多线程写入数据库卡顿,原因未知
        thread_list = []
        for n in range(int(PageMoviesNum)):
            if  ((TitleInfos[n][1]) in databasedic) == False:
                t = threading.Thread(target=mythread, args=(TitleInfos,n,))
                thread_list.append(t)
        for t in thread_list:
            t.start()
            # time.sleep(.5)
        try:
            t.join()
        except:pass

        # 单线程
        # for n in range(int(PageMoviesNum)):
        #     if  ((TitleInfos[n][1]) in databasedic) == False:
        #         mythread(TitleInfos,n)

def getscorecount(text):
    """
    :param text: 评分字符串
    :return: [评分，评分人数]
    """
    text = text.strip()
    regex_dotall = re.compile('(.*?)/10.*?(\d+).*?users', re.S)
    res = regex_dotall.findall(str(text))
    try:
        return [res[0][0],res[0][1]]
    except:
        return [0,0]
    # 8.5/10 from 6203 users
    pass
def getcommentsinfo(url):
    commentlist = []
    CommentsText =geturltext(url)  # 评论数详情
    regex_dotall = re.compile('id="fennum">(\d+)</span>', re.S)
    res = regex_dotall.findall(str(CommentsText))
    try:
        commentlist.append(res[0])
    except  IndexError as e:
        print("{0}解析评论信息失败，正在重试...".format(e))
        getcommentsinfo(url)
    else:
        return commentlist
    pass
def GetSours(url):
    '''
    获取评分
    :param url:
    :return:
    '''
    try:
        res = requests.get(url, headers=headers)
    except:
        print("访问{0}出错,正在重试".format(url))
        time.sleep(3)
        GetSours(url)
    else:
        if res.status_code ==200:
            regex_dotall = re.compile(".*?(\d+).*?", re.S)
            try:
                Sours = regex_dotall.findall(res.text)[0]
                return  Sours
            except:
                return  -1
        else:
            print("请求出错："+str(res.status_code))
            return -1
def getmoviename(text):
    regex_dotall = re.compile('《(.*?)》', re.S)
    res = regex_dotall.findall(str(text))
    try:
        moviename = res[0]
        return [True,moviename]
    except:
        return [False, ""]
def geturltext(url):
    try:
        res = requests.get(url, headers=headers)
    except:
        print("请求出错：{0}\t正在重试".format(url))
        time.sleep(1.5)
        geturltext(url)
    else:
        if res.status_code == 200:
            # print(res.text)
            res.encoding = 'gbk'
            return res.text
        else:
            print("请求出错：{0}\t正在重试".format(url))
            time.sleep(5)
            geturltext(url)
def getmoviedata(htmlstr):
    resultdic = {
        "译名": ["译　　名","英 文 名" ],
        "片名": ["片　　名", ],
        "年代": ["年　　代", ],
        "产地": ["产　　地", "国　　家"],
        "类别": ["电影类型", "类　　别"],
        "语言": ["语　　言", ],
        "字幕": ["字　　幕", ],
        "上映日期": ["上映日期", ],
        "IMDB评分": ["IMDb评分","IMDB评分" ],
        "豆瓣评分": ["豆瓣评分", ],
        "集数": ["集　　数", ],
        "片长": ["片　　长", ],
        "导演": ["导　　演", ],
        "编剧": ["编　　剧", ],
        "主演": ["主　　演", ],
        "简介": ["简　　介", ],
        "获奖情况": ["获奖情况", ]
    }
    try:
        soup = BeautifulSoup(htmlstr, "html.parser")
        content = soup.find(id='endText')
        regex_dotall = re.compile('(.*?)<hr/>', re.S)
        ClassIDs = regex_dotall.findall(str(content))
        soup = BeautifulSoup(ClassIDs[0], "html.parser")
        strlist = soup.text.split("◎")
        for s in strlist:
            for key in resultdic.keys():
                templist = resultdic[key]
                if isinstance(templist, list):
                    for n in templist:
                        if s.find(n) != -1:
                            resultdic[key] = s.lstrip(n)
        for i in resultdic:
            if isinstance(resultdic[i], list):
                resultdic[i] = ""
        return resultdic
    except:
        for i in resultdic:
            resultdic[i] = ""
        return resultdic
def writerdatabase(moviename,sqlstr):
    conn = sqlite3.connect('E:\SQLite\SQLiteData\movies6v.db')
    cursor = conn.cursor()
    try:
        cursor.execute(sqlstr)
        conn.commit()
    except sqlite3.IntegrityError as e:
        print("写入出错：{0}\t{1}".format(moviename, e))
    except sqlite3.OperationalError as e:
        print("写入出错：{0}\t{1}".format(moviename, e))
        # if str(e) == "database is locked":
        #     print("database is locked.正在重试")
        #     time.sleep(.8)
        #     writerdatabase(moviename, sqlstr)
    except:
        print("未知错误：{0}".format(moviename))
    else:
        print("写入数据库：{0}".format(moviename))
    finally:
        cursor.close()
        conn.close()
if __name__ == '__main__':
    HomeUrl = "http://www.hao6v.com/dy/"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive',
        'Host': 'www.hao6v.com',
    }
    result = geturltext(HomeUrl)
    # print()
    regex_dotall = re.compile('class="listpage">页次：<b>1/(\d+)</b>&nbsp;每页<b>(\d+)</b>&nbsp;总数<b>(\d+)</b>&nbsp;', re.S)
    NumInfo = regex_dotall.findall(result)[0]
    print("将要获取网站页面数：{0}\n每页电影数量：{1}\n获取电影总数：{2}".format(NumInfo[0], NumInfo[1], NumInfo[2]))
    main(NumInfo[0],NumInfo[1])
