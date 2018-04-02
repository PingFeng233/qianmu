import threading
import time
import signal
import requests
from lxml import etree
import redis


START_URL = "http://qianmu.iguye.com/2018USNEWS世界大学排名"
# link_queue = Queue()
threads = []
DOWNLOADER_NUM = 10
DOWNLOAD_DELAY=1
downloader_pages = 0
r = redis.Redis()
thread_on = True


def fetch(url, raise_err=False):
    global downloader_pages
    # print(url)
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)
    else:
        downloader_pages += 1
        if raise_err:
            # 返回的状态码不是200,就报错(request自带)
            r.raise_for_status()
    return r.text.replace('\t', '')


def parse(html):
    # global link_queue
    selector = etree.HTML(html)
    links = selector.xpath('//*[@id="content"]/table/tbody/tr/td[2]/a/@href')
    for link in links:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        # link_queue.put(link)
        # sadd方法,往队列里添加,如果队列里已经存在,不再添加,返回结果为0
        if r.sadd('qianmu.seen', link):
            r.lpush('qianmu.queue', link)


def parse_university(html):
    selector = etree.HTML(html)
    table = selector.xpath('//*[@id="wikiContent"]/div[1]/table/tbody')
    if not table:
        return
    table = table[0]
    keys = table.xpath('./tr/td[1]//text()')
    cols = table.xpath('./tr/td[2]')
    values = [' '.join(col.xpath('.//text()')) for col in cols]
    info = dict(zip(keys, values))
    print(info)
    r.lpush('qianmu.items', info)


def downloader(i):
    print('Thread--%s start'%i)
    while True:
        link = r.rpop('qianmu.queue')
        if link:
            link=link.decode('utf-8')
            parse_university(fetch(link))
            print('Thread-%s  %s  remaining queue :%s'%(i,link,r.llen('qianmu.queue')))
        time.sleep(DOWNLOAD_DELAY)
    print('Thread--%s exit now' % i)

def sigint_handler(signum, frame):
    print('Received Ctrl+C,wait for exit gracefully')
    global thread_on
    thread_on = False

if __name__ == '__main__':
    start_time = time.time()
    links = parse(fetch(START_URL, raise_err=True))
    for i in range(DOWNLOADER_NUM):
        t = threading.Thread(target=downloader,args=(i+1,))
        t.start()
        threads.append(t)

    signal.signal(signal.SIGINT, sigint_handler)

    for t in threads:
        t.join()

    cost_seconds = time.time() - start_time
    print('download %s pages,cost_seconds: %s' % (downloader_pages, cost_seconds))