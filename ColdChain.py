import os
import time
import random
import json
import requests
from datetime import datetime

# Basic functions ----------------------------------------
class ColdChain:
    def __init__(self):
        pass

    def get_url(self):
        url = 'https://api.lianku.org.cn/hfivepc/warehouse/queryPage'
        return url
        
    def get_post_datas(self,start_page,end_page):
        post_datas = []
        pageNum_list = [i for i in range(start_page,end_page+1)]
        random.shuffle(pageNum_list)
        for pageNum in pageNum_list:
            post_data = {'searchContent':'','pageNum':str(pageNum),'pageSize':'10','areaName':'','areaId':''}
            post_datas.append(json.dumps(post_data))
        return post_datas
    
    def get_single_post_data(self,pageNum):
        post_data = {'searchContent':'','pageNum':str(pageNum),'pageSize':'10','areaName':'','areaId':''}
        return json.dumps(post_data)
    
    def get_headers(self,post_data):
        content_length = len(post_data)
        headers = {
        'Accept':'application/json, text/plain, */*',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en,zh-CN;q=0.9,zh;q=0.8',
        'Connection':'keep-alive',
        'Content-Length':str(content_length-13),  # length of post_data
        'Content-Type':'application/json;charset=UTF-8',
        'Host':'api.lianku.org.cn',
        'Origin':'http://www.liankur.com',
        'Referer':'http://www.liankur.com/',
        'Sec-Ch-Ua':'Chromium;v=116, Not)A;Brand;v=24, Google Chrome;v=116',
        'Sec-Ch-Ua-Mobile':'?0',
        'Sec-Ch-Ua-Platform':'macOS',
        'Sec-Fetch-Dest':'empty',
        'Sec-Fetch-Mode':'cors',
        'Sec-Fetch-Site':'cross-site',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }
        return headers

    def get_data(self,file_path,start_page,end_page):
        post_datas = self.get_post_datas(start_page,end_page)
        titles = ['id','name','cityName','regionName','address',
                  'rice','longitude','latitude',
                  'rentWarehouseTypeNameList','',
                  'warehouseTypeNameList','','',
                  'score','scoreName','scoreCount',
                  'isShelf','image']
        file = self.create_file(file_path,titles=titles)
        self.grab_pages(post_datas,file_path,file)
        self.redo_fail_posts(file_path,file,redo_counter=3)
    
    def grab_pages(self,post_datas,file_path,file):
        fail_posts = []
        counter = 1
        for post_data in post_datas:
            headers = self.get_headers(post_data)
            url = self.get_url()
            try:
                response = requests.post(url,data=post_data,headers=headers)
                if response.status_code != 200:
                    raise Exception
                datas = json.loads(response.text)
                self.insert_data(file,datas['Tag'])
                print(f'No.<{counter:>3}>. Post <{post_data}> saved.')
                counter += 1
            except Exception as e:
                print(e)
                fail_posts.append(post_data)
                print(f'Post failed. {post_data}')
            time.sleep(random.normalvariate(1.5,0.25))
        self.save_fail_page(file_path,fail_posts)

    def read_fail_posts(self,file_path):
        fail_file = 'fail_log.txt'
        f = open(os.path.join(file_path,fail_file),'r')
        fail_posts = f.readlines()
        if fail_posts == []:
            print('Successfully redone all the failed posts.')
            quit()
        fail_posts = [post.strip() for post in fail_posts]
        fail_posts = [json.loads(post) for post in fail_posts]
        return fail_posts

    def redo_fail_posts(self,file_path,file,redo_limit=1):
        redo_counter = 1
        while redo_counter <= redo_limit:
            fail_posts = self.read_fail_posts(file_path)
            fail_posts = [self.get_single_post_data(post_data['pageNum']) for post_data in fail_posts]
            self.grab_pages(fail_posts,file_path,file)
            redo_counter += 1

    def create_file(self,file_path,file_name='Lianku',titles=None):
        date = datetime.now().date().strftime('%Y%m%d')
        file_name = f'{file_name}_{date}'
        counter = 1
        temp_name = file_name[:]
        while True:
            if temp_name+'.csv' in os.listdir(file_path):
                temp_name = file_name + f'({counter})'
                counter += 1
            else:
                break
        file_name = temp_name[:]
        file = os.path.join(file_path,file_name+'.csv')
        f = open(file,'w',encoding='utf-8-sig')
        if file != None:
            f.write(','.join(titles)+'\n')
        f.close()
        return file
    
    def insert_data(self,file,datas):
        f = open(file,'r+',encoding='utf-8-sig')
        f.readlines()
        use_keys = ['id','name','cityName','regionName',
                    'address','rice','longitude','latitude',
                    'rentWarehouseTypeNameList','warehouseTypeNameList',
                    'score','scoreName','scoreCount','isShelf','image']
        rent_type = ['整租','零租']
        warehose_type = ['冷藏仓','冷冻仓','其它仓']
        for data in datas:
            temp = []
            for key in use_keys:
                if key == 'rentWarehouseTypeNameList':
                    for rent in rent_type:
                        if rent in data[key]:
                            temp.append(rent)
                        else:
                            temp.append('')
                elif key == 'warehouseTypeNameList':
                    for warehouse in warehose_type:
                        if warehouse in data[key]:
                            temp.append(warehouse)
                        else:
                            temp.append('')
                else:
                    temp.append(data[key])
            f.write(','.join([str(s) for s in temp])+'\n')
        f.close()

    def save_fail_page(self,file_path,fail_posts):
        fail_file = 'fail_log.txt'
        f = open(os.path.join(file_path,fail_file),'w')
        f.write('\n'.join(fail_posts))
        f.close()

if __name__ == '__main__':
    file_path = './ColdChain'
    os.makedirs(file_path)
    start_page = 1
    end_page = 712
    Scraper = ColdChain()
    Scraper.get_data(file_path,start_page,end_page)