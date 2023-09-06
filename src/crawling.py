import re
import sys
sys.path.append('.')

import time
import yaml
import aiohttp
import argparse
import asyncio
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from db.dbconn import Database

main_url = 'https://hankookilbo.com'
db_columns = ['title', 'contents', 'author', 'date', 'main_category', 'sub_category', 'source']
columns = ','.join(db_columns)
table = 'news'

db = Database()
async def save(news):
    sql = f"insert into {table} ({columns}) values %s"
    values = [tuple(n.values()) for n in news]
    db.insert_bulk(q=sql, arg=values)
    
def get_last_page_number(html):
    number_regex = re.compile(r'\d+')
    pagination = html.find('div', 'list-paging')
    
    if pagination:
        page_btn = pagination.find_all('a')
        last_page_btn_attrs = page_btn[-1].attrs['href']
        last_page_number = number_regex.search(last_page_btn_attrs).group()
        return int(last_page_number)
    else:
        return 1
    
async def get_html(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={'User-Agent':'Mozilla/5.0'}) as resp:
            html_text = await resp.text()
            html = BeautifulSoup(html_text, 'html.parser')
            return html
        
        
async def get_detail_url(html):
    list_tag = 'ul'
    list_class = 'board-list column-3'
    news_list = html.find(list_tag, list_class)

    list_title_tag = 'h3'
    list_title_class = ''
    news_titles = news_list.find_all(list_title_tag, list_title_class)
    
    detail_url_tag = 'a'
    detail_url_tags = [tag.find(detail_url_tag) for tag in news_titles]

    detail_urls = [tag.attrs['href'] for tag in detail_url_tags if tag]
    detail_urls = [main_url + detail_url for detail_url in detail_urls]

    return detail_urls
    
    
async def get_news(site: str, main_category: str, sub_category: str, search_date: str):
    search_url = f"{main_url}/News/{main_category}/{sub_category}?SortType=&SearchDate={search_date}"
    # search_url = f"{main_url}/News/{main_category}/{sub_category}"
    
    title = ''
    author = ''
    date = ''
    contents = ''
    source = site
    news_list_page_html = await get_html(search_url)
    last_page_number = get_last_page_number(news_list_page_html)
    
    for i in range(1, last_page_number+1):
        search_url += f'&Page={i}'
        news_list = await get_html(search_url)
        news_detail_urls = await get_detail_url(news_list)
        detail_html = (await get_html(url) for url in news_detail_urls)
        news = []
        
        async for html in detail_html:
            title = html.find('h2', 'title').text
            author = html.find('span', 'nm').text
            date = html.find('dl', 'wrt-text').text
            if date:
                date = list(filter(lambda x: x, date.split('\n')))[1]

            contents = ' '.join([tag.text for tag in html.find_all('p', 'editor-p')])
            news.append({
                'title': title.strip(),
                'contents': contents.strip(),
                'author': author.strip(),
                'date': date.strip(),
                'main_category': main_category,
                'sub_category': sub_category,
                'source': source
            })
        await save(news)
    
    
async def run(site, main_category, sub_category, date):
    futures = [asyncio.ensure_future(get_news(site=site, main_category=main_category, sub_category=sub, search_date=date)) for sub in sub_category]
    await asyncio.gather(*futures)


def main():
    # start time
    start = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--site', dest='site', help='ex. chosun, joonang, donga, seoul, khan, maeil, hani, hankook')
    parser.add_argument('-c', '--category', dest='category', help='ex. politics, economy, international, society, culture, entertinament, sports')
    parser.add_argument('-d', '--date', dest='date', help='ex. 20230901')
    args = parser.parse_args()
    site = args.site
    category = args.category
    date = args.date
    
    with open('config/category.yaml', 'r') as f:
        cate_config = yaml.load(f, Loader=yaml.FullLoader)

    if site is None:
        raise KeyError(f"'site' is None")
    
    if category is None:
        raise KeyError(f"'category' is None")
    
    if category in cate_config[site]:
        main_category = list(cate_config[site][category].keys())[0]
        sub_category = cate_config[site][category][main_category]
    else:
        raise KeyError(f"expect 'politics', 'economy', 'international', 'society', 'culture', 'entertainment', 'sports', but got {category}")
    
    
    print(main_category, sub_category, date)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(site, main_category, sub_category, date))
    loop.close()
    
    # end time
    end = time.time()
    running_time = end - start
    print(f"running time: {running_time:.4f}s")
    
if __name__ == '__main__':
    main()
