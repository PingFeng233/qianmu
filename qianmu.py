import requests
from lxml import etree

START_URL = "http://qianmu.iguye.com/2018USNEWS世界大学排名"
link_queue = []


def fetch(url):
    print(url)
    r = requests.get(url)
    return r.text.replace('\t', '')


def parse(html):
    global link_queue
    selector = etree.HTML(html)
    links = selector.xpath('//*[@id="content"]/table/tbody/tr/td[2]/a/@href')
    link_queue += links


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


if __name__ == '__main__':
    links = parse(fetch(START_URL))
    for link in link_queue:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/%s' % link
        parse_university(fetch(link))
