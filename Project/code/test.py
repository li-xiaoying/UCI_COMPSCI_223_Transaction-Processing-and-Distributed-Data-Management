import requests
import time
import json
import psycopg2
from typing import Callable, Dict, List, Set
# url = 'http://localhost:30000/request'
# logfile = "/home/linqi/295-project/YCSB/ycsb_trace/workloada.Log"
# loading_len = 1000
# max_range = 10
# Batches = process(logfile, loading_len, max_range)

# payload = dict(
#     id = (0, 0),
#     client_url = ""
# )
headers = {'content-type': 'application/json'}
# data = [['READ','cross','1']]
# # batch = Batches[0]
# payload = {
#     'id': (0, 32),
#     'client_url': 'http://localhost:20001/reply',
#     'timestamp': time.time(),
#     'data': data
# }

# payload2 = {
#     "action" : "view change"
# }

# for i in range(4):
#     urli = 'http://localhost:3000'+ str(i) + '/view_change_request'
#     #ri = requests.post(urli, data=json.dumps(payload2), headers=headers)
# #url2 = 'http://localhost:30000/view_change_request'

# print(time.time())
# #r = requests.post(url, data=json.dumps(payload2), headers=headers)
# #r2 = requests.post(url2, data=json.dumps(payload2), headers=headers)

# snap: Dict[str, str] = {}
# snap['0'] = 't'
# payload3 = {
#     'operation' : 'a',
#     'client_url': 'http://localhost:20001',
#     't_index': '0'
# }
# writeset = []
# writeset.append('a')
# writeset.append('b')
# payload4 = {
#     'writeset' : writeset,
#     'valuewriteset' : {'a':1,'b':2},
#     'client_url': 'http://localhost:20001',
#     't_index': '0'
# }
# #url = 'http://localhost:30000/rrequest'
# url4 = 'http://localhost:30000/commit'
# ri = requests.post(url4, data=json.dumps(payload4), headers=headers)

# snap: Dict[str, str] = {}


# payload5 = {
#     'operation' : 'a',
#     'client_url': 'http://localhost:20001',
#     't_index': '0'
# }

# url5 = 'http://localhost:30001/get_readonly_request'
# ri = requests.post(url5, data=json.dumps(payload5), headers=headers)

# payload6 = {
#     'operation' : 'a',
#     'client_url': 'http://localhost:20001',
#     't_index': '0'
# }

# url6 = 'http://localhost:30001/leader_change_request'
# url7 = 'http://localhost:30002/leader_change_request'
# ri = requests.post(url6, data=json.dumps(payload6), headers=headers)
# ri = requests.post(url7, data=json.dumps(payload6), headers=headers)

# waitlist: List[str] = list()
# waitlist.append('0')
# waitlist.append('1')
# payload8 = {
#     'waitlist' : waitlist,
#     'client_url': 'http://localhost:20001',

# }

# url8 = 'http://localhost:30001/state'

# ri = requests.post(url8, data=json.dumps(payload8), headers=headers)

def test1():
    # Single transaction test (read a, write b = 1, read c, commit)
    # T0 start
    # T0 read a
    # T0 write b = 1
    # T0 read c
    # T0 commit

    writeset = []
    valuewriteset: Dict[str, int] = {}
    # read a
    payload = {
    'operation' : 'a',
    'client_url': 'http://localhost:20001',
    't_index': '0'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # write b
    writeset.append('b')
    valuewriteset['b'] = 1
    # read c
    payload = {
    'operation' : 'c',
    'client_url': 'http://localhost:20001',
    't_index': '0'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # commit
    payload = {
    'writeset' : writeset,
    'valuewriteset' : valuewriteset,
    'client_url': 'http://localhost:20001',
    't_index': '0'
    }
    url = 'http://localhost:30000/commit'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

def test2():
    # Concurrent transactions test (Disjoint)(T1 <read a, write b = 2, read c, commit>, T2 <read a, write d = 1, read c, commit>)
    # T1 start
    # T1 read a
    # T1 write b = 2
    # T2 start
    # T2 read a
    # T2 write d = 1
    # T2 read c
    # T2 commit
    # T1 read c
    # T1 commit
    # Since these transactions are disjoint, they should be all commit successful.

    writeset1 = []
    valuewriteset1: Dict[str, int] = {}

    writeset2 = []
    valuewriteset2: Dict[str, int] = {}

    # T1 read a
    payload = {
    'operation' : 'a',
    'client_url': 'http://localhost:20001',
    't_index': '1'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T1 write b = 2
    writeset1.append('b')
    valuewriteset1['b'] = 2

    # T2 read a
    payload = {
    'operation' : 'a',
    'client_url': 'http://localhost:20001',
    't_index': '2'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T2 write d = 1
    writeset2.append('d')
    valuewriteset2['d'] = 1

    # T2 read c
    payload = {
    'operation' : 'c',
    'client_url': 'http://localhost:20001',
    't_index': '2'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T2 commit
    # T2 should be commit successfully
    payload = {
    'writeset' : writeset2,
    'valuewriteset' : valuewriteset2,
    'client_url': 'http://localhost:20001',
    't_index': '2'
    }
    url = 'http://localhost:30000/commit'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T1 read c
    payload = {
    'operation' : 'c',
    'client_url': 'http://localhost:20001',
    't_index': '1'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T1 commit
    # T1 should be commit successfully
    payload = {
    'writeset' : writeset1,
    'valuewriteset' : valuewriteset1,
    'client_url': 'http://localhost:20001',
    't_index': '1'
    }
    url = 'http://localhost:30000/commit'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

def test3():
    # Concurrent transactions test (joint)(T3 <read a, write b = 3, read c, commit>, T4 <read c, write a = 1, read b, commit>)
    # T3 start
    # T3 read a
    # T3 write b = 3
    # T4 start
    # T4 read c
    # T4 write a = 1
    # T4 read b
    # T4 commit
    # T3 read c
    # T3 commit
    # Since these transactions are not disjoint, here T4 can commit, but T3 should be abort.
    writeset3 = []
    valuewriteset3: Dict[str, int] = {}

    writeset4 = []
    valuewriteset4: Dict[str, int] = {}

    # T3 read a
    payload = {
    'operation' : 'a',
    'client_url': 'http://localhost:20001',
    't_index': '3'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T3 write b = 3
    writeset3.append('b')
    valuewriteset3['b'] = 3

    # T4 read c
    payload = {
    'operation' : 'c',
    'client_url': 'http://localhost:20001',
    't_index': '4'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T4 write a = 1
    writeset4.append('a')
    valuewriteset4['a'] = 1

    # T4 read b
    payload = {
    'operation' : 'b',
    'client_url': 'http://localhost:20001',
    't_index': '4'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T4 commit
    # T2 should be commit successfully
    payload = {
    'writeset' : writeset4,
    'valuewriteset' : valuewriteset4,
    'client_url': 'http://localhost:20001',
    't_index': '4'
    }
    url = 'http://localhost:30000/commit'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T3 read c
    payload = {
    'operation' : 'c',
    'client_url': 'http://localhost:20001',
    't_index': '3'
    }
    url = 'http://localhost:30000/rrequest'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # T3 commit
    # T3 should be abort
    payload = {
    'writeset' : writeset3,
    'valuewriteset' : valuewriteset3,
    'client_url': 'http://localhost:20001',
    't_index': '3'
    }
    url = 'http://localhost:30000/commit'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

def test4():
    # Replication log test


    # Single transaction test (read a, write b = 1, read c, commit)
    # T0 start
    # T0 read a
    # T0 write b = 1
    # T0 read c
    # T0 commit
    test1()
    # check node 0 database
    index = 0
    dbname = "table" + str(index)
    connect_str = "host={} dbname={} user={} password={}".format('localhost', dbname, 'linqi', '123')
    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()
    name = 'b'
    cur.execute("SELECT * FROM test WHERE name=%s", name)
    row = cur.fetchone()
    print(f'{row[0]} {row[1]}')

    # check node 1 database
    index = 1
    dbname = "table" + str(index)
    connect_str = "host={} dbname={} user={} password={}".format('localhost', dbname, 'linqi', '123')
    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()
    name = 'b'
    cur.execute("SELECT * FROM test WHERE name=%s", name)
    row = cur.fetchone()
    print(f'{row[0]} {row[1]}')

    # check node 2 database
    index = 2
    dbname = "table" + str(index)
    connect_str = "host={} dbname={} user={} password={}".format('localhost', dbname, 'linqi', '123')
    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()
    name = 'b'
    cur.execute("SELECT * FROM test WHERE name=%s", name)
    row = cur.fetchone()
    print(f'{row[0]} {row[1]}')

def test5():
    #Read-only transaction test (T5 <read a>)
    payload = {
    'operation' : 'a',
    'client_url': 'http://localhost:20001',
    't_index': '5'
    }
    # send to node 0
    url = 'http://localhost:30000/get_readonly_request'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # send to node 1
    url = 'http://localhost:30001/get_readonly_request'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

    # send to node 2
    url = 'http://localhost:30002/get_readonly_request'
    test = requests.post(url, data=json.dumps(payload), headers=headers)

def test6():
    # leader change test
    # Here we simulate the leader change case by sending the leader change request to node 1 and node 2
    payload = {
    'operation' : 'a',
    'client_url': 'http://localhost:20001',
    't_index': '0'
    }
    # send to node 1 and node 2
    url1 = 'http://localhost:30001/leader_change_request'
    url2 = 'http://localhost:30002/leader_change_request'
    test1 = requests.post(url1, data=json.dumps(payload), headers=headers)
    test2 = requests.post(url2, data=json.dumps(payload), headers=headers)

    # after received new leader node index, here we suppose it's node 1,
    # we should recover transaction state by sending transaction state request to the new leader node
    # In this request, we suppose transaction 0 and 1 hasn't been commit or abort
    waitlist: List[str] = list()
    waitlist.append('0')
    waitlist.append('1')
    payload = {
        'waitlist' : waitlist,
        'client_url': 'http://localhost:20001',
    }

    url = 'http://localhost:30001/state'
    test3 = requests.post(url, data=json.dumps(payload), headers=headers)


def main():
    test1()
    test2()
    test3()
    test4()
    test5()
    test6()
if __name__ == "__main__":
    main()