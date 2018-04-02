import threading
import time
from queue import Queue
import requests
from lxml import etree

START_URL = "http://qianmu.iguye.com/2018USNEWS世界大学排名"
link_queue = Queue()
threads = []
DOWNLOADER_NUM = 10
downloader_pages = 0


def fetch(url, raise_err=False):
    global downloader_pages
    print(url)
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
    global link_queue
    selector = etree.HTML(html)
    links = selector.xpath('//*[@id="content"]/table/tbody/tr/td[2]/a/@href')
    for link in links:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        link_queue.put(link)


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


def downloader():
    while True:
        link = link_queue.get()
        if link is None:
            break
        parse_university(fetch(link))
        link_queue.task_done()
        print('remaining queue: %s' % link_queue.qsize())


if __name__ == '__main__':
    start_time = time.time()
    links = parse(fetch(START_URL, raise_err=True))
    for i in range(DOWNLOADER_NUM):
        t = threading.Thread(target=downloader)
        t.start()
        threads.append(t)
    link_queue.join()

    for i in range(DOWNLOADER_NUM):
        link_queue.put(None)

    for t in threads:
        t.join()

    cost_seconds = time.time() - start_time
    print('download %s pages,cost_seconds: %s' % (downloader_pages, cost_seconds))