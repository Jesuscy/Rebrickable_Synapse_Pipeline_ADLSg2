from bs4 import BeautifulSoup
import requests
import os
import zipfile
 
url = 'https://rebrickable.com/downloads/'
res = requests.get(url)
soup = BeautifulSoup(res.text, 'html.parser')
body = soup.find('body')
wrapper = body.find('div', {'id': 'wrapper'})
content = wrapper.find('div', {'id': 'content'})
container = content.find('div', {'class': 'container'})
row = container.find('div', {'class': 'row'})
col = row.find('div', {'class': 'col-md-5'})
ul = col.find('ul', {'class': 'list-unstyled'})
li = ul.find_all('li')

for i in li:
    span = i.find('span')
    a = span.find('a')
    link = a['href']        