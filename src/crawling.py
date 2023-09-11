import re
import sys
sys.path.append('.')
import time
import yaml
import aiohttp
import argparse
import asyncio
from bs4 import BeautifulSoup
from db.dbconn import Database

# 뉴스메인url
main_url = 'https://hankookilbo.com'
# db컬럼정보
db_columns = ['title', 'contents', 'author', 'date', 'main_category', 'sub_category', 'source']
columns = ','.join(db_columns)

# db 테이블명
table = 'news'

db = Database()
async def save(news: list) -> None:
    """
    크롤링된 뉴스를 데이터베이스 저장하기 위한 함수

    Args:
        news (list): YYYYMMDD날짜에 조회된 모든 뉴스가 담긴 리스트
        ex) [{ ... }, { ... }, ... ]
    """
    # bulk insert를 수행하기 위한 sql과 파라미터 생성
    sql = f"insert into {table} ({columns}) values %s"
    values = [tuple(n.values()) for n in news]
    
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
    pagination = html.find('div', 'list-paging')

    # 페이징이 있다는 것은 2페이지 이상 존재
    # 페이징이 없는 것은 뉴스가 1페이지만 존재하거나 뉴스가 없는 경우
    if pagination:
        # 버튼 부분에서 숫자만 추출
        page_btn = pagination.find_all('a')
        last_page_btn_attrs = page_btn[-1].attrs['href']
        last_page_number = number_regex.search(last_page_btn_attrs).group()
        page_number = int(last_page_number)
    
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
    list_tag = 'ul'
    
    # 뉴스 리스트 html의 클래스명
    list_class = 'board-list column-3'
    
    # 뉴스 리스트 html 내용
    news_list = html.find(list_tag, list_class)

    # 뉴스 제목의 태그명
    list_title_tag = 'h3'
    
    # 뉴스 제목의 클래스명
    list_title_class = ''
    
    # 뉴스 제목 html내용
    news_titles = news_list.find_all(list_title_tag, list_title_class)
    
    # 뉴스 리스트 내의 상세페이지 태그
    detail_url_tag = 'a'
    detail_url_tags = [tag.find(detail_url_tag) for tag in news_titles]

    # 상세페이지 태그가 존재할 경우 그 안에서 href속성값을 추출하여 url로 사용한다.
    detail_urls = [tag.attrs['href'] for tag in detail_url_tags if tag]
    detail_urls = [main_url + detail_url for detail_url in detail_urls]

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
    search_url = f"{main_url}/News/{main_category}/{sub_category}?SortType=&SearchDate={search_date}"

    # 뉴스정보 초기값 할당
    title = ''
    author = ''
    date = ''
    contents = ''
    source = site
    
    # 뉴스리스트 html
    news_list_page_html = await get_html(search_url)
    
    # 뉴스리스트 마지막 페이지 번호
    last_page_number = get_last_page_number(news_list_page_html)
    
    # 뉴스페이지가 여러 개 존재할 수 있으므로 페이지 별로 크롤링 수행
    for i in range(1, last_page_number+1):
        
        # 뉴스 검색 url에 페이지 번호 querystring을 추가해준다.
        search_url += f'&Page={i}'
        
        # 페이지별 조회된 뉴스 리스트 html
        news_list = await get_html(search_url)
        
        # 상세페이지 url
        news_detail_urls = await get_detail_url(news_list)
        
        # 상세페이지 html
        detail_html = (await get_html(url) for url in news_detail_urls)
        news = []

        async for html in detail_html:
            try:
                # 뉴스제목
                title = html.find('h2', 'title').text
                # 기자이름
                author = html.find('span', 'nm').text
                # 뉴스일자
                date = html.find('dl', 'wrt-text').text
                # 뉴스내용에 입력시간과 수정시간이 같이 추출되기때문에 '입력시간'만 뉴스일자로 사용하기 위함
                if date:
                    date = list(filter(lambda x: x, date.split('\n')))[1]
                # 뉴스 본문
                contents = ' '.join([tag.text for tag in html.find_all('p', 'editor-p')])
                # db에 저장하기 위해 뉴스내용을 딕셔너리 형태로 구성하고 조회된 모든 뉴스를 리스트에 담아서 반환하도록 함
                news.append({
                    'title': title.strip(),
                    'contents': contents.strip(),
                    'author': author.strip(),
                    'date': date.strip(),
                    'main_category': main_category,
                    'sub_category': sub_category,
                    'source': source
                })
            except Exception as e:
                pass
                
        try:
            # YYYYMMDD일자의 모든 뉴스들에 대한 크롤링이 완료되면 리스트에 담긴 모든 뉴스를 bulk형태로 insert 수행함
            await save(news)
        except Exception as e:
            print(e)
            
            
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
    await asyncio.gather(*futures)


def main() -> None:
    """
    뉴스 크롤링 entrypoint

    Raises:
        KeyError: 지원하지 않는 뉴스 사이트인 경우 에러 발생
        KeyError: 지원하지 않는 뉴스 카테고리인 경우 에러 발생
    """
    # 프로그램 시작 시간
    start = time.time()
    
    # 프로그램 실행에 필요한 인자생성
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--site', dest='site', help='ex. chosun, joonang, donga, seoul, khan, maeil, hani, hankook')
    parser.add_argument('-c', '--category', dest='category', help='ex. politics, economy, international, society, culture, entertinament, sports')
    parser.add_argument('-d', '--date', dest='date', help='ex. 20230901')
    
    args = parser.parse_args()
    
    # 뉴스사이트 
    site = args.site
    # 뉴스카테고리
    category = args.category
    # 뉴스일자
    date = args.date
    
    # 카테고리별 상세카테고리가 정의되어 있는 yaml파일
    with open('config/category.yaml', 'r') as f:
        cate_config = yaml.load(f, Loader=yaml.FullLoader)

    # 지원하지 않는 사이트이거나 오타인 경우 keyerror발생
    if site is None:
        raise KeyError(f"'site' is None")
    
    # 지원하지 않는 뉴스카테고리거나 오타인 경우 keyerror발생
    if category is None:
        raise KeyError(f"'category' is None")
    
    if category in cate_config[site]:
        main_category = list(cate_config[site][category].keys())[0]
        sub_category = cate_config[site][category][main_category]
    else:
        raise KeyError(f"expect 'politics', 'economy', 'international', 'society', 'culture', 'entertainment', 'sports', but got {category}")
    
    
    print(main_category, sub_category, date)
    
    # 비동기 이벤트루프 생성
    loop = asyncio.get_event_loop()
    # 비동기 함수 수행
    loop.run_until_complete(run(site, main_category, sub_category, date))
    # 비동기 이벤트루프 종료
    loop.close()
    
    # 프로그램 종료시간
    end = time.time()
    
    # 프로그램 수행시간
    running_time = end - start
    print(f"running time: {running_time:.4f}s")
    
if __name__ == '__main__':
    main()
