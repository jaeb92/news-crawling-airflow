import yaml
import pandas as pd
from typing import Any

class Config:
    def __init__(self, site, category):
        
        with open('config/category.yaml', 'r') as f:
            categories = yaml.load(f, Loader=yaml.FullLoader)
            self.category = categories[site][category]
            self.main_category = list(self.category.keys())[0]
            self.sub_categories = self.category[self.main_category]
        
        with open('config/url.yaml', 'r') as f:
            url = yaml.load(f, Loader=yaml.FullLoader)
            self.url = url[site]
            self.urls = {
                'base': self.url['base'],
                'endpoint': self.url['list_page_endpoint']
            }
            
            
        with open('config/tag.yaml', 'r') as f:
            tag = yaml.load(f, Loader=yaml.FullLoader)
            self.tag = tag[site]
            
            self.tags = {
                'list_tag': self.tag['list']['tag'],
                'list_class': self.tag['list']['class'],
                'list_title_tag': self.tag['list_title']['tag'],
                'list_title_class': self.tag['list_title']['class'],
                'detail_url_tag': self.tag['detail_url']['tag'],
                'detail_url_attrs': self.tag['detail_url']['attrs'],
                'title_tag': self.tag['title']['tag'],
                'title_class': self.tag['title']['class'],
                'contents_tag': self.tag['contents']['tag'],
                'contents_class': self.tag['contents']['class'],
                'author_tag': self.tag['author']['tag'],
                'author_class': self.tag['author']['class'],
                'date_tag': self.tag['date']['tag'],
                'date_class': self.tag['date']['class'],
                'image_tag': self.tag['image']['tag'],
                'image_class': self.tag['image']['class']   
            }
            
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.main_category, self.sub_categories, self.urls, self.tags
        