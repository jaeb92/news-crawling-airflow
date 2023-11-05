import re
import aiohttp
import requests
import json
from pprint import pprint
from bs4 import BeautifulSoup

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
headers = {
    'User-Agent': user_agent
}
cookies = {
    'SID': "bAjUT1JificNkToIJWsEg34PMgE-muC4Y_5R-z4gVsWpYZmkRzK_wXAGB7b4TjBFQ5-OMg."
}        

# url = "https://www.chosun.com/politics/diplomacy-defense/"
url = "https://www.chosun.com/national/transport-environment/"
res = requests.get(url, headers=headers, cookies=cookies)
soup = BeautifulSoup(res.text, 'html.parser')

matched = re.search(r'{"site-service"(.+?);', str(news_list_page_html), re.S)
print(matched)


# script_tags = soup.find('script', id='fusion-metadata')
# print(script_tags.find(re.compile()))
# for i, script_tag in enumerate(script_tags):
#     print(script_tag)
    # json.loads(script_tag.text)
    # print(script_tag.find('Fusion.globalContent'))
    # with open(f"script_{i}.html", 'w') as f:
    #     f.write(script_tag.find('story-feed'))
# news_list = BeautifulSoup(html.body.script.text, 'html.parser')
# print(news_list)
# with open('result.html', 'w') as f:
#     f.write(str(html))
# with aiohttp.ClientSession() as session:
#     with session.get(url, headers={'User-Agent':'Mozilla/5.0'}) as resp:
#         html_text = resp.text()
#         html = BeautifulSoup(html_text, 'html.parser')

# pprint(html.text)