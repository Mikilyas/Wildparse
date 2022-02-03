import asyncio
import datetime
import json
import time
from urllib import parse

import aiohttp
import motor.motor_asyncio
from celery import Celery
from celery.schedules import crontab

STEP = 500
WILDBERRIES_BASE_URL = 'https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog'


app = Celery('tasks', broker='redis://localhost:6379')

app.conf.beat_schedule = {
    'load-data-every-hour': {
        'task': 'tasks.plane_download_data',
        'schedule': crontab(hour='*/1', minute='05'),
        'args': (100000, 100002, 3000, 2)
    },
}


def calc_intervals(start, end, num):
    seq = list(range(start, end))
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        inner = seq[int(last):int(last + avg)]
        if len(inner) == 1:
            out.append([inner[0], inner[0]])
        elif inner:
            out.append([inner[0], inner[-1]])
        last += avg
    return out


def gen_checkpoints(start, stop, step):
    current = start
    while current + step < stop:
        yield current, current + step
        current += step
    yield current, stop


def make_id_for_mongo(product_id, date, hour):
    product_id = str(product_id)
    product_id = '0' * (8 - len(product_id)) + product_id
    return f'{product_id}{date}{hour}'


async def download_products(client, start_product_id, end_product_id, date, hour):
    for i in range(10):
        try:
            async with client.get(f'{WILDBERRIES_BASE_URL}?{parse.urlencode({"nm": ";".join([str(i) for i in range(start_product_id, end_product_id)]), "locale": "ru"})}') as response:
                assert response.status == 200
                data = await response.read()
                data = json.loads(data.decode('utf-8'))
                break
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientOSError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as e:
            print(e)
            await asyncio.sleep(5)
    return [{'_id': make_id_for_mongo(product['id'], date, hour), 'data': product, 'timestamp': int(time.time())} for product in data['data']['products']]


async def download_all_products(start_product_id, end_product_id, chunk_size, date, hour):
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient()
    db = mongo_client.wild_db
    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession(loop=loop, headers={'Connection': 'keep-alive'}) as client:
        for current_start_chunk, current_end_chunk in gen_checkpoints(start_product_id, end_product_id, chunk_size):
            products_data = await asyncio.gather(
                *[download_products(client, *checkpoint, date, hour) for checkpoint in gen_checkpoints(current_start_chunk, current_end_chunk, STEP)]
            )
            chunk_products = []
            for products in products_data:
                chunk_products.extend(products)
            if chunk_products:
                await db.products.insert_many(chunk_products)
    mongo_client.close()


@app.task
def download_data(start_product_id, end_product_id, chunk_size, date, hour):
    asyncio.run(download_all_products(start_product_id, end_product_id, chunk_size, date, hour))


@app.task
def plane_download_data(start_product_id, end_product_id, chunk_size, processes):
    date = datetime.datetime.now()
    hour = str(date.hour)
    date = date.strftime('%d%m%Y')
    intervals = calc_intervals(start_product_id, end_product_id, processes)
    for interval in intervals:
        download_data.delay(interval[0], interval[1], chunk_size, date, hour)
