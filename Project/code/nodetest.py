import argparse
import logging
from random import random
import yaml
import asyncio
import aiohttp
from aiohttp import web
from typing import Callable, Dict, List, Set
from database import CachingDatabaseWrapper, Database, Optional, Transaction

class Handler:
    RREQUEST = 'rrequest'
    COMMIT = 'commit'
    LOG_SYNC = 'log'
    READONLY = 'get_readonly_request'
    LEADER_CHANGE_REQUEST = 'leader_change_request'
    LEADER_EXCHANGE_REQUEST = 'leader'
    STATE = 'state'

    def __init__(self, index, conf, db):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._index = index
        # Number of faults tolerant.


        # leader

        self._next_propose_slot = 0
        # LocalDB
        self._db = db
        self._replic_log = []
        # Locallocker
        self._commit: Set[str] = set()
        # partkey
        self._snap: Dict[str, SerialTransactionExecutor] = {}

        # TODO: Test fixed
        if self._index == 0:
            self._is_leader = True
        else:
            self._is_leader = False

        # Network simulation
        self._loss_rate = conf['loss%'] / 100

        # Time configuration
        self._network_timeout = conf['misc']['network_timeout']

        # Checkpoint

        # After finishing committing self._checkpoint_interval slots,
        # trigger to propose new checkpoint.

        # Commit
        self._last_commit_slot = -1

        # Indicate my current leader.
        # TODO: Test fixed
        self._leader = 0

        # The largest view either promised or accepted

        # Restore the votes number and information for each view number
        self._view_change_votes_by_view_number = {}
        # Record signature for each view number
        self._view_change_votes_signed_by_view_number = {}
        # Record all the status of the given slot
        # To adjust json key, slot is string integer.
        self._status_by_slot = {}

        self._sync_interval = conf['sync_interval']
 
        
        self._session = None
        self._log = logging.getLogger(__name__) 


            
    @staticmethod
    def make_url(node, command):
        '''
        input: 
            node: dictionary with key of host(url) and port
            command: action
        output:
            The url to send with given node and action.
        '''
        return "http://{}:{}/{}".format(node['host'], node['port'], command)

    async def _make_requests(self, nodes, command, json_data):
        '''
        Send json data:

        input:
            nodes: list of dictionary with key: host, port
            command: Command to execute.
            json_data: Json data.
        output:
            list of tuple: (node_index, response)

        '''
        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                if not self._session:
                    timeout = aiohttp.ClientTimeout(self._network_timeout)
                    self._session = aiohttp.ClientSession(timeout=timeout)
                self._log.debug("make request to %d, %s", i, command)
                try:
                    resp = await self._session.post(self.make_url(node, command), json=json_data)
                    resp_list.append((i, resp))
                    
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass
        return resp_list 

    async def _make_response(self, resp):
        '''
        Drop response by chance, via sleep for sometime.
        '''
        if random() < self._loss_rate:
            await asyncio.sleep(self._network_timeout)
        return resp

    async def _post(self, nodes, command, json_data):
        '''
        Broadcast json_data to all node in nodes with given command.
        input:
            nodes: list of nodes
            command: action
            json_data: Data in json format.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("make request to %d, %s", i, command)
                try:
                    _ = await self._session.post(self.make_url(node, command), json=json_data)
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass
    
    async def _postclient(self, url, command, json_data):
        '''
        Broadcast json_data to all node in nodes with given command.
        input:
            nodes: list of nodes
            command: action
            json_data: Data in json format.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        
        if random() > self._loss_rate:
            try:
                _ = await self._session.post(url + "/{}".format(command), json=json_data)
            except Exception as e:
                #resp_list.append((i, e))
                self._log.error(e)
                pass


    async def get_read_request(self, request):
        '''
        Handle the request from client if leader, otherwise 
        redirect to the leader.
        '''
        def incr_vars(vs: str) -> Transaction:
            def txn(db: CachingDatabaseWrapper) -> None:
                db.read(vs)
            return txn
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        vs = json_data['operation']
        client_url = json_data['client_url']
        transaction_index = json_data['t_index']
        incr_v = incr_vars(vs)
        if transaction_index not in self._snap:
            t = self._db.begin(incr_v)
            t.read_phase()
            self._snap[transaction_index] = t
            result = self._snap[transaction_index].cached_db.read(vs)

        else:
            result = self._snap[transaction_index].cached_db.read(vs)
        
        result_msg = {
                    'leader': self._index,
                    't_index': transaction_index,
                    'result': result,
                    'type': 'read',
                    'operation': vs
                }
        await self._postclient(client_url, 'reply', result_msg)
        
        return web.Response()

    async def get_commit_request(self, request):
        '''
        Handle the request from client if leader, otherwise 
        redirect to the leader.
        '''
        flag = 0
        def incr_vars(ws: List[str],vws: Dict[str, int]) -> Transaction:
            def txn(db: CachingDatabaseWrapper) -> None:
                for w in ws:
                    db.write(w, vws[w])
            return txn
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        ws = json_data['writeset']
        vws = json_data['valuewriteset']
        client_url = json_data['client_url']
        transaction_index = json_data['t_index']

        incr_v = incr_vars(ws, vws)
        if transaction_index not in self._snap:
            t = self._db.begin(incr_v)
            t.read_phase()
            self._snap[transaction_index] = t

        else:
            for w in ws:
                self._snap[transaction_index].cached_db.write(w, vws[w])
        
        finish_tn = self._db._get_tnc()
        for tn in range(self._snap[transaction_index].start_tn + 1, finish_tn + 1):
            cached_db = self._db._get_transaction(tn)
            write_set = cached_db.get_write_set()
            read_set = self._snap[transaction_index].cached_db.get_read_set()
            if not write_set.isdisjoint(read_set):
                flag = 1
        
        if flag == 0:
            self._db._commit_transaction(self._snap[transaction_index].cached_db)
            result = 'commit'
            self._replic_log.append(self._snap[transaction_index].cached_db.copies)
            await self.log_sync(self._snap[transaction_index].cached_db.copies, transaction_index)
            self._commit.add(transaction_index)
        else:
            result = 'abort'

        result_msg = {
                    'leader': self._index,
                    't_index': transaction_index,
                    'result': result,
                    'type': 'commit'
                }

        await self._postclient(client_url, 'reply', result_msg)
        
        return web.Response()

    async def log_sync(self, copies, transaction_index):

        log_msg = {
                    'leader': self._index,
                    't_index': transaction_index,
                    'log': copies,
                    'type': 'commitlog'
                }
        print(log_msg)
        if self._index == 0:
            post_addr = []
            post_addr.append(self._nodes[1])
            post_addr.append(self._nodes[2])
            await self._post(post_addr, 'log',log_msg)
        elif self._index == 1:
            post_addr = []
            post_addr.append(self._nodes[0])
            post_addr.append(self._nodes[2])
            await self._post(post_addr, 'log',log_msg)
        else:
            post_addr = []
            post_addr.append(self._nodes[0])
            post_addr.append(self._nodes[1])
            await self._post(post_addr, 'log',log_msg)
        
        

    async def get_log(self, request):
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        t_index = json_data['t_index']
        log = json_data['log']
        self._replic_log.append(log)
        def incr_vars(vws: Dict[str, int]) -> Transaction:
            def txn(db: CachingDatabaseWrapper) -> None:
                for k, v in vws.items():
                    db.write(k, v)
            return txn
        incr_v = incr_vars(log)
        t = self._db.begin(incr_v)
        t.read_phase()
        self._db._commit_transaction(t.cached_db)
        self._commit.add(t_index)

        return web.Response()

    async def get_readonly_request(self, request):
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        activeset = self._replic_log[-1]
        writeset = set(activeset.keys())
        readset = json_data['operation']
        client_url = json_data['client_url']
        transaction_index = json_data['t_index']
        if  writeset.isdisjoint(readset):
            def incr_vars(vs: str) -> Transaction:
                def txn(db: CachingDatabaseWrapper) -> None:
                    db.read(vs)
                return txn

            incr_v = incr_vars(readset)
            if transaction_index not in self._snap:
                t = self._db.begin(incr_v)
                t.read_phase()
                self._snap[transaction_index] = t
                result = self._snap[transaction_index].cached_db.read(readset)

            else:
                result = self._snap[transaction_index].cached_db.read(readset)
        else:
            result = 'abort'
        
        result_msg = {
                    'node': self._index,
                    't_index': transaction_index,
                    'result': result,
                    'operation': readset,
                    'type': 'readonly'
                }

        await self._postclient(client_url, 'reply',result_msg)
        
        return web.Response()

    async def get_leader_change_request(self, request):
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        client_url = json_data['client_url']
        result_msg = {
                    'node': self._index,
                    'loglen': len(self._replic_log),
                    'type': 'leader_change',
                    'client_url': client_url
                }
        if self._index == 1:
            post_addr = []
            post_addr.append(self._nodes[2])
            await self._post(post_addr, 'leader',result_msg)
        else:
            post_addr = []
            post_addr.append(self._nodes[1])
            await self._post(post_addr, 'leader',result_msg)
        
        return web.Response()
        
    async def get_leader_exchange(self, request):
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        if self._index == 1:
            if json_data['loglen'] > len(self._replic_log):
                self._leader = 2
            else:
                self._leader = 1
        else:
            if json_data['loglen'] >= len(self._replic_log):
                self._leader = 1
            else:
                self._leader = 2

        if self._leader == self._index:
            result_msg = {
                    'node': self._index,
                    'loglen': len(self._replic_log),
                    'type': 'leader_elected',
                }
            await self._postclient(json_data['client_url'], 'reply', result_msg)
        
        return web.Response()

    async def get_state_request(self, request):
        self._log.info("---> %d: on request", self._index)
        json_data = await request.json()
        waitlist = json_data['waitlist']
        result = []
        for item in waitlist:
            if item not in self._commit:
                result.append('abort')
            else:
                result.append('commit')
        
        result_msg = {
                    'node': self._index,
                    'waitlist': waitlist,
                    'result': result,
                    'type': 'state',
                }
        
        await self._postclient(json_data['client_url'], 'reply', result_msg)
        
        return web.Response()

def conf_parse(conf_file) -> dict:
    '''
    nodes:
        - host: localhost
          port: 30000
        - host: localhost
          port: 30001
        - host: localhost
          port: 30002


    clients:
        - host: localhost
          port: 20001


    loss%: 0

    ckpt_interval: 10

    retry_times_before_view_change: 2

    sync_interval: 5

    misc:
        network_timeout: 5
    '''
    conf = yaml.load(conf_file)
    return conf

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
    parser = argparse.ArgumentParser(description='BackUp Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    parser.add_argument('-lf', '--log_to_file', default=False, type=bool, help='Whether to dump log messages to file, default = False')
    args = parser.parse_args()
    return args

class SerialTransactionExecutor:
    def __init__(self, db: 'SerialDatabase', txn: Transaction) -> None:
        self.db = db
        self.cached_db = CachingDatabaseWrapper(db)
        self.txn = txn
        self.start_tn = self.db._get_tnc()

    def read_phase(self) -> None:
        self.txn(self.cached_db)

    def validate_and_write_phase(self) -> bool:
        finish_tn = self.db._get_tnc()
        for tn in range(self.start_tn + 1, finish_tn + 1):
            cached_db = self.db._get_transaction(tn)
            write_set = cached_db.get_write_set()
            read_set = self.cached_db.get_read_set()
            if not write_set.isdisjoint(read_set):
                return False
        self.db._commit_transaction(self.cached_db)
        return True

class SerialDatabase(Database):
    def __init__(self,index) -> None:
        Database.__init__(self,index)
        self.transactions: Dict[int, CachingDatabaseWrapper] = {}
        self.tnc: int = 0

    def _get_tnc(self) -> int:
        return self.tnc

    def _get_transaction(self, tn: int) -> CachingDatabaseWrapper:
        assert tn in self.transactions
        return self.transactions[tn]

    def _commit_transaction(self, db: CachingDatabaseWrapper) -> None:
        self.tnc += 1
        assert self.tnc not in self.transactions
        self.transactions[self.tnc] = db
        db.commit()

    def begin(self, txn: Transaction) -> SerialTransactionExecutor:
        return SerialTransactionExecutor(self, txn)

def main():
    args = arg_parse()
    if args.log_to_file:
        logging.basicConfig(filename='log_' + str(args.index),
                            filemode='a', level=logging.DEBUG)
    logging_config()
    log = logging.getLogger()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['nodes'][args.index]
    host = addr['host']
    port = addr['port']

    
    db = SerialDatabase(args.index)
    backup = Handler(args.index, conf, db)

    

    
    app = web.Application()
    app.add_routes([
        web.post('/' + Handler.RREQUEST, backup.get_read_request),
        web.post('/' + Handler.COMMIT, backup.get_commit_request),
        web.post('/' + Handler.LOG_SYNC, backup.get_log),
        web.post('/' + Handler.READONLY, backup.get_readonly_request),
        web.post('/' + Handler.LEADER_CHANGE_REQUEST, backup.get_leader_change_request),
        web.post('/' + Handler.LEADER_EXCHANGE_REQUEST, backup.get_leader_exchange),
        web.post('/' + Handler.STATE, backup.get_state_request),


        ])

    web.run_app(app, host=host, port=port, access_log=None)
    

if __name__ == "__main__":
    main()