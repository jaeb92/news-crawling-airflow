def pagination():
    pass

def detail_urls(site, detail_url_tags):
    detail_urls = [tag.attrs[tags['detail_url']['attrs']] for tag in detail_url_tags if tag]

    if site == 'hankook':
        detail_urls = [main_url + detail_url for url_list in detail_urls]
    
    return detail_urls

