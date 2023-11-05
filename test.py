import requests
import re
import json

with open('config/entities.txt', 'r') as f:
    html_entity = f.readlines()
    html_entity = [entity.strip() for entity in html_entity]
    
# url = 'https://askdjango.github.io/lv3/'
for i in range(1,3):
    
    # url = f"https://www.chosun.com/politics/diplomacy-defense/?page={i}"
    url = f"https://www.chosun.com/national/transport-environment/?page={i}"
    print('url:', url, '============================\n\n\n')
    html = requests.get(url).text
    # print(html)
    html = re.sub('|'.join(html_entity), '', str(html))
    matched = re.search(r'{"site-service"(.+?);', html, re.S)
    json_str = matched.group()
    # print(json_str)
    json_obj = json.loads(json_str[:-1])
    feed = json_obj['story-feed']
    news = feed[list(feed.keys())[0]]['data']['content_elements']
    # print(news)
    for data in news:
        news_id = data['_id']
        canonical_url = data['canonical_url']
        headlines = data['headlines']['basic']
        description = data['description']['basic']
        author = data['credits']['by']
        author = list(map(lambda x: x['additional_properties']['original']['byline'], author))
        print('news id:', news_id)
        print('canonical_url:', canonical_url)
        print('headlines:', headlines)
        print('description:', description)
        print('author:', author)
        print()
# for 


# t = '''{"data":{"contents_elements":{}}}'''
# print(t)
# print(json.loads(t))