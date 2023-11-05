import re
import sys
sys.path.append('.')
import json
import time
import yaml
import pprint
import random
import aiohttp
import asyncio
import traceback
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from db.dbconn import Database
from modules.commons import detail_urls
from logger.loggings import LoggerFactory

logger = LoggerFactory.get_logger(log_file="news_crawling", log_level="DEBUG")


async def save(table: str, columns: str, data: list) -> None:
    """
    크롤링된 뉴스를 데이터베이스 저장하기 위한 함수

    Args:
        news (list): YYYYMMDD날짜에 조회된 모든 뉴스가 담긴 리스트
        ex) [{ ... }, { ... }, ... ]
    """
        
    # bulk insert를 수행하기 위한 sql과 파라미터 생성
    sql = f"insert into {table} ({columns}) values %s"
    values = [tuple(n.values()) for n in data]
    
    # bulk insert 수행
    db.insert_bulk(q=sql, arg=values)
    
def get_last_page_number(html: str) -> int:
    """
    뉴스의 마지막 페이지 번호를 반환하는 함수

    Args:
        html (str): 뉴스리스트페이지의 html 문자열

    Returns:
        int: 뉴스의 마지막 페이지 번호
    """
    
    # 뉴스 리스트 페이지 기본값 할당
    page_number = 1
    
    # 페이징 html태그에서 숫자부분만 추출하기 위한 정규표현식
    number_regex = re.compile(r'\d+')
    
    # 페이징 html부분 찾기
    # pagination = html.find('div', 'list-paging')
    
    pagination_tag = site_meta['tags']['pagination']
    pagination = html.find(pagination_tag['tag'], pagination_tag['class'])
    # 페이징이 있다는 것은 2페이지 이상 존재
    # 페이징이 없는 것은 뉴스가 1페이지만 존재하거나 뉴스가 없는 경우
    if pagination:
        # page_btn = find_last_pagination(pagination)
        # 버튼 부분에서 숫자만 추출
        page_btn = pagination.find_all('a')
        try:
            last_page_btn_attrs = page_btn[-1].attrs['href']
            last_page_number = number_regex.search(last_page_btn_attrs).group()
            page_number = int(last_page_number)
        except Exception as e:
            page_number = 1
    # 페이징이 있으면 가장 마지막 페이지의 숫자를 int타입으로 반환
    # 그렇지 않으면 최초 할당한 1이 반환됨
    return page_number


async def get_html(url: str) -> BeautifulSoup:
    """
    url주소의 html내용을 반환하는 함수

    Args:
        url (str): 크롤링할 페이지의 url주소

    Returns:
        BeautifulSoup: html태그
    """

    # 여러 개의 상세카테고리에 대한 request 요청을 비동기로 수행하도록 한다.
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={'User-Agent':'Mozilla/5.0'}) as resp:
            html_text = await resp.text()
            html = BeautifulSoup(html_text, 'html.parser')
            return html
            
        
async def get_detail_url(html: str) -> list:
    """
    상세페이지의 url주소를 반환하는 함수

    Args:
        html (str): 뉴스 리스트페이지의 html 내용

    Returns:
        list: 조회된 뉴스의 상세페이지 url이 담긴 리스트
    """
    # 뉴스 리스트 html의 태그명    
    list_tag = tags['list']['tag']
    
    # 뉴스 리스트 html의 클래스명
    list_class = tags['list']['class']

    news_list = html.find_all(list_tag, list_class)
    if not news_list:
        return []
    
    # 뉴스 제목의 태그명
    list_title_tag = tags['list_title']['tag']
    
    # 뉴스 제목의 클래스명
    list_title_class = tags['list_title']['class']
    
    # 뉴스 제목 html내용
    news_titles = news_list.find_all(list_title_tag, list_title_class)
    
    # 뉴스 리스트 내의 상세페이지 태그
    detail_url_tag = tags['detail_url']['tag']
    detail_url_tags = [tag.find(detail_url_tag) for tag in news_titles]

    # 상세페이지 태그가 존재할 경우 그 안에서 href속성값을 추출하여 url로 사용한다.
    detail_urls = [tag.attrs[tags['detail_url']['attrs']] for tag in detail_url_tags if tag]
    detail_urls = [site_meta['url'] + detail_url if site_meta['url'] not in detail_url else detail_url for detail_url in detail_urls]
    
    return detail_urls
    
    
async def get_news(site: str, main_category: str, sub_category: str, search_date: str) -> None:
    """
    뉴스의 상세내용을 크롤링하는 함수

    Args:
        site (str): 뉴스 사이트명 (ex. hankook(한국일보), ... )
        main_category (str): 뉴스 메인 카테고리 (ex. Politics, Economy, International, Society, Culture, Sports)
        sub_category (str): 뉴스 상세 카테고리 (ex. HB01, HB02 ... )
        search_date (str): YYYYMMDD 형태의 뉴스일자 (ex. 20230101)
    """
    # 뉴스 검색 url
    # search_url = f"{main_url}/News/{main_category}/{sub_category}?SortType=&SearchDate={search_date}"
    # logger.info(f"{main_url}, {main_category}, {sub_category}, s")    
    date_querystring = site_meta['date_querystring'] if site_meta['date_querystring'] else ''
    search_url = main_url + '/' + main_category + '/' + sub_category + '/?'  # 하위카테고리의 url
    # logger.info(f'search url: {search_url}')
    
    # 뉴스정보 초기값 할당
    title = ''
    author = ''
    date = ''
    contents = ''
    source = site
    
    try:
        logger.info(f"search url: {search_url}")
        news_list_page_html = await get_html(search_url)
        
        # html entity 제거
        news_list_page_html = re.sub('|'.join(html_entity), '', str(news_list_page_html))
        
        matched = re.search(r'{"site-service"(.+?);', str(news_list_page_html), re.S)
        matched_str = matched.group()
        # print(matched_str)
        count = 0
        # 정규표현식 마지막 세미콜론을 제외하고 dict형태로 변환
        matched_obj = json.loads(matched_str[:-1])

        with open(f'result/matched_{main_category}_{sub_category}_{int(random.random()*100000)}.json', 'w') as f:
            json.dump(matched_obj, f, ensure_ascii=False, indent=2)
            
        news_feed = matched_obj['story-feed']
        news = news_feed[list(news_feed.keys())[0]]['data']['content_elements']
        
        for data in news:
            count+=1
            news_id = data['_id']
            canonical_url = data['canonical_url']
            headlines = data['headlines']['basic']
            description = data['description']['basic']
            author = data['credits']['by']
            author = list(map(lambda x: x['additional_properties']['original']['byline'] if 'byline' in x else '', author))
            # print('news id:', news_id)
            # print('canonical_url:', canonical_url)
            print('headlines:', headlines)
            # print('description:', description)
            # print('author:', author)
            # print()
    except Exception as e:
        # logger.info(f"{matched_str}")
        with open(f'result/matched_{main_category}_{sub_category}_{int(random.random()*100000)}.txt', 'w') as f:
            f.write(str(news_list_page_html))
            
        traceback.print_exc()
    # 뉴스리스트 마지막 페이지 번호
    # last_page_number = get_last_page_number(news_list_page_html)
    
    # 뉴스페이지가 여러 개 존재할 수 있으므로 페이지 별로 크롤링 수행
    # count = 0
    # # exit()
    # print('last page number:', last_page_number)
    # for i in range(1, last_page_number+1):
    #     # 뉴스 검색 url에 페이지 번호 querystring을 추가해준다.
    #     pq = site_meta['page_querystring'] if site_meta['page_querystring'] else ''
    #     page_url = search_url + "&" + f"{site_meta['page_querystring']}{i}"
    #     print('pageurl:', page_url)
    #     # 페이지별 조회된 뉴스 리스트 html
    #     news_list = await get_html(page_url)
        
    #     print('news list:', news_list.text)
    #     # 상세페이지 url
    #     news_detail_urls = await get_detail_url(news_list)
    #     # 상세페이지 html
    #     detail_html = (await get_html(url) for url in news_detail_urls)
    #     news = []

    #     async for html in detail_html:
    #         try:
    #             # 뉴스제목
    #             title = html.find(tags['title']['tag'], tags['title']['class']).text
                
    #             # 기자이름
    #             author = html.find(tags['author']['tag'], tags['author']['class'])
    #             if author:
    #                 try:
    #                     author = author.find('strong').text
    #                 except:
    #                     author = author.text
    #             else:
    #                 author = ''
                    
    #             # 뉴스일자
    #             date = html.find(tags['date']['tag'], tags['date']['class']).text
    #             # 뉴스내용에 입력시간과 수정시간이 같이 추출되기때문에 '입력시간'만 뉴스일자로 사용하기 위함
                
    #             if date:
    #                 try:
    #                     date = list(filter(lambda x: x, date.split('\n')))[1].replace('\n', '')
    #                 except:
    #                     date = date.split(' ', 1)[-1].replace('\n', '')
                                    
    #             contents = ' '.join([tag.text for tag in html.find_all(tags['contents']['tag'], tags['contents']['class'], limit=1)])

    #             news.append({
    #                 'title': title.strip(),
    #                 'contents': contents.strip(),
    #                 'author': author.strip(),
    #                 'date': date.strip(),
    #                 'main_category': main_category,
    #                 'sub_category': sub_category,
    #                 'source': source
    #             })
                
    #             # logger.info(f'title: {title}')
    #             # logger.info(f'author: {author}')
    #             # logger.info(f'date: {date}')
    #             # logger.info(f'contents: {contents[:20]}')
    #             count += 1
    #         except Exception as e:
    #             logger.error(traceback.format_exc())
            
        # try:
        #     # YYYYMMDD일자의 모든 뉴스들에 대한 크롤링이 완료되면 리스트에 담긴 모든 뉴스를 bulk형태로 insert 수행함
        #     await save(table='news', columns=news_columns, data=news)
        # except Exception as e:
        #     print(e)
            
    # news_insert_count_columns = 'main_category,sub_category,news_date,count'
    # news_insert_count = [{
    #     'main_category': main_category,
    #     'sub_category': sub_category,
    #     'news_date': search_date,
    #     'count': count
    # }]
    # await save(table='news_insert_count', columns=news_insert_count_columns, data=news_insert_count)
    logger.info(f"{main_category}, {sub_category}, {count}")

            
async def run(site: str, main_category: str, sub_category: str, date: str) -> None:
    """
    비동기로 크롤링을 수행하도록 하는 함수

    Args:
        site (str): 뉴스사이트 명
        main_category (str): 뉴스 메인 카테고리
        sub_category (str): 뉴스 상세 카테고리
        date (str): 뉴스 일자
    """
    futures = [asyncio.ensure_future(get_news(site=site, main_category=main_category, sub_category=sub, search_date=date)) for sub in sub_category]
    result = await asyncio.gather(*futures)
    return result


if __name__ == '__main__':
    site = 'chosun'
    date = pd.date_range(start='20230930', end='20230930')
    date_list = list(map(lambda x: datetime.strftime(x, '%Y%m%d'), date))
    category_list = ['politics', 'economy', 'international', 'society', 'culture', 'entertainment', 'sports']
    
    logger.info(site)
    logger.info(category_list)
    # 카테고리별 상세카테고리가 정의되어 있는 yaml파일
    with open('config/category.yaml', 'r') as f:
        cate_config = yaml.load(f, Loader=yaml.FullLoader)
    
    with open('config/tag.yaml', 'r') as f:
        site_meta = yaml.load(f, Loader=yaml.FullLoader)[site]
    
    with open('config/entities.txt', 'r') as f:
        html_entity = f.readlines()
        html_entity = [entity.strip() for entity in html_entity]
        
    tags = site_meta['tags']
    # 뉴스인url
    main_url = site_meta['url'] + site_meta['news_querystring'] if site_meta['news_querystring'] else site_meta['url']
    print('main url:', main_url)
    db = Database()

    # db컬럼정보
    news_columns = ['title', 'contents', 'author', 'date', 'main_category', 'sub_category', 'source']
    news_columns = ','.join(news_columns)

    # db 테이블명
    table = 'news'
    # print(site_tags)
    
    # logger.info(f"{cate_config[site]}")
    # 비동기 이벤트루프 생성
    # category_list = ['society']
    loop = asyncio.get_event_loop()
    for date in date_list:
        # 프로그램 시작 시간
        start = time.time()
        for category in category_list:
            logger.info(f"{date}, {category}")
            if category in cate_config[site]:
                main_category = list(cate_config[site][category].keys())[0]
                # main_category = 'sports'
                sub_category = cate_config[site][category][main_category]
                # sub_category = ['transport-environment']
            else:
                raise KeyError(f"expect 'politics', 'economy', 'international', 'society', 'culture', 'entertainment', 'sports', but got {category}")

            # 비동기 함수 수행
            loop.run_until_complete(run(site, main_category, sub_category, date))
            # 비동기 이벤트루프 종료
            
            # 프로그램 종료시간
            end = time.time()
            
        # 프로그램 수행시간
        running_time = end - start
        logger.info(f"running time: {running_time:.4f}s")
    
    loop.close()