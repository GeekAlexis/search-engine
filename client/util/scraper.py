from bs4 import BeautifulSoup
import json
import requests
import time


def get_urls(base_url):
    urls = []
    res = requests.get(base_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    table = soup.find(
        'section', {'id': 'copyright-area-widget-text-6'}).find_all('td')

    for td in table:
        a = td.find('a')
        if a is not None:
            urls.append(a['href'])

    return urls


def scrape(url, dct):
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    table = soup.find('table').find_all('tr')[1:]

    for tr in table:
        tds = tr.find_all('td')
        if len(tds) != 5:
            continue
        query = tds[1].text
        cnt = tds[2].text
        try:
            dct[query] = int(cnt.replace(',', ''))
        except ValueError:
            continue


def main():
    base_url = 'https://www.mondovo.com/keywords'
    urls = get_urls(base_url)
    visited = set(base_url)
    dct = {}

    for url in urls:
        if url in visited:
            continue
        visited.add(url)
        try:
            scrape(url, dct)
        except:
            continue
        time.sleep(2)

    sorted_dct = sorted(dct.items(), key=lambda x: x[1], reverse=True)
    queries = [x[0] for x in sorted_dct]
    data = {'data': queries}
    with open('topQueries.json', 'w') as f:
        json.dump(data, f)


if __name__ == '__main__':
    main()
