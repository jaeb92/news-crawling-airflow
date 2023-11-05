import yaml
import re
with open('config/entities.txt', 'r') as f:
    html_entity = f.readlines()
    html_entity = [entity.strip() for entity in html_entity]
        

# s = '이 전했다. 윤 대통령은 이어 직원들과 환담&amp; &heart;을 하고 “우리 경제가 더 성장하고 발전하기 위해서는 5000만 내수 시장으로는 부족하기 때문에 수출과 수입을 더 늘릴 수밖에 없다”고  말했다.&nbsp;그러면서 “항공 화물 없이는 국민 경제 활동도 없는 것이나 마찬가지”라며 “여러분이 계셔서 나라 경제도 돌아가는 것이기 때문에 자부심을 갖고 더 열심히 해주기를 바란다”고 했다.&nbsp;윤 대통령은 직원들과..."},"display_date":"2023-09-28T12:12:56.498Z","headlines":{"basic":"尹대통령, 인천공항 화물터미널 근로자 격려... “여러분 있어 나라 경제 돌아'
s = 'jwehef&amp;as'
res = re.search(r'&[a-zA-Z]+;', s)
print(res.group())
# res = re.sub('|'.join(html_entity), '', s)
# print(html_entity)
# print(res)