#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
import json
import asyncio
import aiohttp
from aiohttp import web
from random import random
import hashlib


def logging_config(log_level=logging.INFO, log_file=None):
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return

    root_logger.setLevel(log_level)

    f = logging.Formatter("[%(levelname)s]%(module)s->%(funcName)s: \t %(message)s \t --- %(asctime)s")

    h = logging.StreamHandler()
    h.setFormatter(f)
    h.setLevel(log_level)
    root_logger.addHandler(h)

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        h = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        h.setFormatter(f)
        h.setLevel(log_level)
        root_logger.addHandler(h)

def arg_parse():
    # parse command line options
    parser = argparse.ArgumentParser(description='PBFT Node')
    parser.add_argument('-id', '--client_id', type=int, help='client id')
    parser.add_argument('-nm', '--num_messages', default=10, type=int, help='number of message want to send for this client')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    '''
    Sample config:

    nodes:
        - host: 
          port:
        - host:
          port:

    loss%:

    skip:

    heartbeat:
        ttl:
        interval:

    election_slice: 10

    sync_interval: 10

    misc:
        network_timeout: 10
    '''
    conf = yaml.load(conf_file)
    return conf

def make_url(node, command):
    return "http://{}:{}/{}".format(node['host'], node['port'], command)

class Client:
    REQUEST = "request"
    REPLY = "reply"
    VIEW_CHANGE_REQUEST = 'view_change_request'

    def __init__(self, conf, args, log):
        self._nodes = conf['nodes']
        self._resend_interval = conf['misc']['network_timeout']
        self._client_id = args.client_id
        self._num_messages = args.num_messages
        self._session = None
        self._address = conf['clients'][self._client_id]
        self._client_url = "http://{}:{}".format(self._address['host'], 
            self._address['port'])
        self._log = log

        self._retry_times = conf['retry_times_before_view_change']
        # Number of faults tolerant.
        self._f = (len(self._nodes) - 1) // 3

        # Event for sending next request
        self._is_request_succeed = asyncio.Event()
        # To record the status of current request


    async def get_reply(self, request):

        json_data = await request.json()
        if json_data['type'] == 'read':
            variable = json_data['operation']
            transaction_index = json_data['t_index']
            result = json_data['result']
            self._log.info("---> %d received transaction %s (read request about %s) reply, the result is %d.", self._client_id, transaction_index, variable, result)

        elif json_data['type'] == 'commit':
            transaction_index = json_data['t_index']
            result = json_data['result']
            self._log.info("---> %d received transaction %s (commit or abort) reply, the result is %s.", self._client_id, transaction_index, result)
        
        elif json_data['type'] == 'readonly':
            variable = json_data['operation']
            transaction_index = json_data['t_index']
            result = json_data['result']
            if result != 'abort':
                self._log.info("---> %d received transaction %s (read-only request about %s) reply, the result is %d.", self._client_id, transaction_index, variable, result)
            else:
                self._log.info("---> %d received transaction %s (read-only request about %s) reply, the result is %s.", self._client_id, transaction_index, variable, result)
        
        elif json_data['type'] == 'leader_elected':
            leadernode = json_data['node']
            self._log.info("---> %d received leader change message, the new leader is %d.", self._client_id, leadernode)

        else:
            waitlist = json_data['waitlist']
            result = json_data['result']
            i = 0
            for item in waitlist:
                self._log.info("---> %d received transaction %s (state) reply, the result is %s.", self._client_id, item, result[i])
                i+=1
        
        return web.Response()

def main():
    logging_config()
    log = logging.getLogger()
    args = arg_parse()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['clients'][args.client_id]
    log.info("begin")
    

    client = Client(conf, args, log)

    addr = client._address
    host = addr['host']
    port = addr['port']


    app = web.Application()
    app.add_routes([
        web.post('/' + Client.REPLY, client.get_reply),
    ])

    web.run_app(app, host=host, port=port, access_log=None)


    
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(client.request())

if __name__ == "__main__":
    main()

            
    
