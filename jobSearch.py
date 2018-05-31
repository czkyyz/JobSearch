# -*- coding:utf-8 -*- 
from urllib import request             
from urllib.parse import quote         
import datetime                        
import time                            
from bs4 import BeautifulSoup, element 
import redis
import hashlib
from apscheduler.schedulers.blocking import BlockingScheduler     
import os

pool = None
fp = None
timerange = ''
max_page = 100
origincCity = 'XX'
originKey = 'XX'
city = quote(origincCity)
key = quote(originKey)

head = {}                              
head['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36'

def init():
  global pool
  pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)

def beforeSave():
  global fp
  fp = open('record.txt', 'a', encoding='utf-8')

def afterSave():
  global fp
  fp.close()

def updateRecord(record):
  md5 = hashlib.md5()
  md5.update(record.encode('utf-8'))
  companyMD5 = md5.hexdigest()

  r = redis.Redis(connection_pool=pool)
  if r.sadd('companys', companyMD5) == 1:
    global fp
    fp.write(record + '\n')
    print('[+] ' + record)
    return True
  return False

def getTimeRange():                                    
  today = datetime.date.today()                        
  oneday = datetime.timedelta(days=1)                  

  global timerange                                     
  timerange = today.strftime('%Y%m%d') + '_' + tomorrow.strftime('%Y%m%d')  
def getURL(type):
  return {
    '58': 'http://huizhou.58.com/job/{0}?key=%s&final=1&jump=1&postdate=%s' %(key, timerange),
    'zhilian': 'https://sou.zhaopin.com/jobs/searchresult.ashx?jl=%s&kw=%s&sm=0&pd=1&p=' %(city, key),
    '51job': 'https://search.51job.com/list/030300,000000,0000,00,0,99,%s,2,{0}.html' %quote(key),
    'jobbaidu': 'http://mail.jobbaidu.com/Commons/ListPosition!list.shtml?searcher.jobLocation1=3007&searcher.jobPostDate=1&searcher.indexKeyType=0&searcher.indexKey=%s&page.currentPage=' %key
  }.get(type, '')

# .zwmc indexof(keyword)

def genResponse(url, encoding='utf-8'):
  try:
    req = request.Request(url, headers = head)
    response = request.urlopen(req)
  except urllib.error.URLError as e:
    print(u'urlopen: %s失败, Error: %s' %(url, e.reason))
    return None
  content = response.read().decode(encoding, "ignore")
  return BeautifulSoup(content, 'lxml')

# ***************** 58jb *****************
def run_58job():
  url = getURL('58').format('')
  pages = fetch58Job(url, get_pages=True)

  if pages is 1:
    return

  for page in range(2, pages+1):
    pn = 'pn{0}/'.format(page)
    url = getURL('58').format(pn)
    fetch58Job(url)
    time.sleep(0.5)

def fetch58Job(url, get_pages=False):
  soup = genResponse(url)

  if soup is None:
    return 1

  for li_job in soup.find_all('li', class_='job_item'):
    firstItemCon = li_job.div
    if firstItemCon.div['__addition'] == '1':
      break
    job_name = firstItemCon.div.a.get_text()
    secondItemCon = firstItemCon.next_sibling
    job_company = secondItemCon.div.a['title']

    updateRecord(job_company)

  if get_pages:
    elm_total_page = soup.find('i', class_='total_page')
    return 1 if elm_total_page is None else int(elm_total_page.get_text())

# ***************** 58jb *****************

# ***************** zhilian *****************
def run_zhilian():
  for index in range(1, max_page+1):
    hasRecord = False
    url = getURL('zhilian') + str(index)
    # print(url)
    soup = genResponse(url)

    for index, li_job in enumerate(soup.find_all('table', class_='newlist')):
      if index is 0:
        continue
      firstIn = True
      for a_elm in li_job.find_all('a', attrs={'target': '_blank'}):
        if firstIn:
          if a_elm.get_text().find(originKey) < 0:
            break
          else:
            firstIn = False
            continue

        job_company = a_elm.get_text().strip()
        if updateRecord(job_company):
          hasRecord = True
        break
    if hasRecord is False:
      break

    time.sleep(0.5)
        
# ***************** zhilian *****************

# ***************** 51job *****************

def run_51job():
  for index in range(1, max_page+1):
    hasRecord = False
    url = getURL('51job').format(str(index))
    # print(url)
    soup = genResponse(url, encoding='gbk')
    result_list = soup.find(id='resultList')
    for index, li_job in enumerate(result_list.find_all('div', class_='el')):
      if index is 0:
        continue

      location = li_job.find('span', class_='t3').get_text()

      if location != origincCity:
        continue

      firstIn = True
      for a_elm in li_job.find_all('a', attrs={'target': '_blank'}):
        if firstIn:
          if a_elm['title'].find(originKey) < 0:
            break
          else:
            firstIn = False
            continue

        job_company = a_elm['title'].strip()
        if updateRecord(job_company):
          hasRecord = True
        break
    if hasRecord is False:
      break

    time.sleep(0.5)
        
# ***************** 51job *****************

# ***************** jobbaidu *****************

def run_jobbaidu():
  for index in range(1, max_page+1):
    hasRecord = False
    url = getURL('jobbaidu') + str(index)
    # print(url)
    soup = genResponse(url)
    search_list = soup.find('div', class_='warpSearchList')
    for li_job in search_list.find_all('li', class_='clearfix'):
      firstIn = True
      for a_elm in li_job.find_all('a', attrs={'target': '_blank'}):
        if firstIn:
          if a_elm.span.get_text().find(originKey) < 0:
            break
          else:
            firstIn = False
            continue

        job_company = a_elm['title'].strip()
        if updateRecord(job_company):
          hasRecord = True
        break
    if hasRecord is False:
      break

    time.sleep(0.5)
        
# ***************** jobbaidu *****************

def main():
  getTimeRange()                             
  beforeSave()
  print('Fetching 58job data...')             
  run_58job()
  print('Fetching zhilian data...')
  run_zhilian()
  print('Fetching 51job data...')
  run_51job()
  print('Fetching jobbaidu data...')
  run_jobbaidu()
  afterSave()

if __name__ == '__main__':
  init()
  scheduler = BlockingScheduler()              
  scheduler.add_job(main, 'cron', minute='*/1',hour='*')          
  print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
  try:
    scheduler.start()                          
  except KeyboardInterrupt:
    scheduler.shutdown()                      