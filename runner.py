import os
import time
import asyncio
import http.client
from collector.log_source import query_stream
from aggregator import Aggregator
from collections import defaultdict


SUMO_SECRET = os.environ['SUMO_SECRET']

def send_log(msg):
    try:
        url = 'endpoint2.collection.sumologic.com'
        path = '/receiver/v1/http/{}'.format(os.environ['SUMO_SECRET'])
        conn = http.client.HTTPSConnection(url)
        conn.request('POST', path, body=msg)
        resp = conn.getresponse()
        if resp.status != 200:
            print('HTTP {}: {}'.format(resp.status, resp.read(200)))
    except Exception as e:
        print(e)


def count_selects(rec, state):
    if rec['query'] is None:
        return state

    q = rec['query'].upper()
    if q.startswith('SELECT '):
        table_start = q.find(' FROM ')+6
        table_end = q.find(' WHERE ')
        if table_start != -1:
            key = q[table_start:table_end]
            if key[0] == 'T':  # only record metrics for dataset tables
                key = key.split('_')[0]
                state[key] += 1
    return state


async def log_processor(log, agg):
    count = 100
    for rec in log:
        if rec:
            agg.process(rec)
            count -= 1
            if count < 0:
                count = 100
                await asyncio.sleep(0)
        else:
            count = 100
            await asyncio.sleep(30)


async def read_aggregates(aggs):
    while True:
        await asyncio.sleep(60)
        for a in aggs:
            buffer = ''
            for k,v in a.sample().items():
                rec = {}
                rec['type'] = 'count_selects'
                rec['db'] = a.db
                rec['name'] = k
                rec['value'] = v
                buffer += '{}\n'.format(rec)
            print(buffer)
            send_log(buffer)


if __name__ == '__main__':
    databases = ['pg1-a-staging', 'pg2-staging']
    aggs = []

    for db in databases:
        log = query_stream(db, (time.time()*1000))
        agg = Aggregator(db, count_selects, lambda: defaultdict(int))
        aggs.append(agg)
        asyncio.ensure_future(log_processor(log, agg))

    asyncio.ensure_future(read_aggregates(aggs))
    asyncio.get_event_loop().run_forever()
