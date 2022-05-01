import requests
import json
from bs4 import BeautifulSoup
import random
import time
from mysqlhelper import MySqLHelper

if __name__ == '__main__':

    type_arr = ['电视剧', '电影']
    tv_type_detail_arr = ['热门', '美剧', '英剧', '韩剧', '日剧', '国产剧', '港剧', '日本动画', '综艺', '纪录片']
    movie_type_detail_arr = ['热门', '最新', '经典', '豆瓣高分', '冷门佳片', '华语', '欧美', '韩国', '日本', '动作', '喜剧', '爱情', '科幻', '悬疑',
                             '恐怖', '成长']
    url = 'https://movie.douban.com/j/search_subjects'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'}

    mysqlhelper = MySqLHelper()
    for type_detail in tv_type_detail_arr:
        for i in range(999):
            params = {'type': type_arr[0], 'tag': type_detail, 'page_limit': 200, 'page_start': i}
            response = requests.get(url, params, headers=headers)
            movie_arr = json.loads(response.text)['subjects']
            db_data_arr = []
            if(len(movie_arr)==0):
                break
            for movie in movie_arr:
                offset = 0
                url = movie['url']
                time.sleep(random.randint(1, 5))
                strhtml = requests.get(url, headers=headers)
                soup = BeautifulSoup(strhtml.text, 'lxml')
                data = soup.select('#info > .pl')

                db_data = [movie['id'], movie['title'], type_arr[0], type_detail]
                for item in data:
                    if ('类型' in item.text):
                        db_data.append(item.nextSibling)
                for item in data:
                    if ('集数' in item.text):
                        db_data.append(item.nextSibling)
                if (len(db_data) != 5):
                    db_data.append(None)
                db_data.append(None)
                db_data.append('SYSTEM')
                db_data.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                db_data.append('SYSTEM')
                db_data.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                db_data_arr.append(tuple(db_data))

            sql = 'insert into movie (ID,NAME,TYPE,TYPE_DETAIL,EPISODES,TIME,CREATE_BY,CREATE_TIME,UPDATE_BY,UPDATE_TIME) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            mysqlhelper.insertmany(sql, db_data_arr)
            print('数据插入成功')

# data = soup.select('#content > div > div.article > div.search-result > div:nth-child(3) > div')
# for item in data:
#     type = item.select_one('div.content > div > h3 > span:nth-child(1)')
#     content = item.select_one('div.content > div > h3 > a')
#     if (type and content and ('电视剧' in type.get_text() or '电影' in type.get_text())):
#         print(type.get_text())
#         print(content.get_text())
