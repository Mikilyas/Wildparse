import time
from kataloges import *
from bs4 import BeautifulSoup
import requests
import aiohttp
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent import futures


def parse():
    comps = []
    for key, values in katalogs.items():
        for pk in range(0, 3):
            podkatalog = values[pk]
            URL = mainURL + key + podkatalog
            data1 = [(pagenumber, URL, comps, podkatalog, values, pk) for pagenumber in range(1, 300)]
            # usersloop = await asyncio.gather(*[loop_through_pages(elem) for elem in data1])
            workers = min(64, 16)
            with futures.ProcessPoolExecutor(max_workers=8) as executor:
                executor.map(loop_through_pages, data1)


def loop_through_pages(data1):
    pagenumber, URL, comps, podkatalog, values, pk = data1[0], data1[1], data1[2], data1[3], data1[4], data1[5]
    if pagenumber >= 3:
        if 'pages' in URL:
            URL = URL.replace(pages + str(pagenumber - 1), pages + str(pagenumber))
        else:
            URL = URL.replace(pages + str(pagenumber - 1), pages + str(pagenumber)) + pages + str(pagenumber)
    elif pagenumber == 1:
        URL = URL
    elif pagenumber == 2:
        URL = URL + pages + str(pagenumber)

    # loop = asyncio.get_event_loop()
    # future1 = loop.run_in_executor(None, requests.get, URL, HEADERS)
    # response = await future1
    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    items = soup.findAll('div', class_='dtList-inner')

    if len(items) == 0:
        return

    for item in items:
        try:
            comps.append({
                'title': item.find('span', class_='goods-name c-text-sm').get_text(strip=True),
                'price': item.find('span', class_='price').get_text(strip=True),
                'link': item.find('a', class_='ref_goods_n_p j-open-full-product-card').get('href')
            })
        except:
            pass

    global comp
    for comp in comps:
        print(f'{comp["title"]} -> Price: {comp["price"]} -> Link: {comp["link"]}')
    print(podkatalog + " => " + str(values[pk]) + " => " + str(pagenumber))


# asyncio.run(parse())
parse()
