from typing import Any, Dict, Optional, Set, Callable
import psycopg2



class Database:
    """
    A database is a mapping from string-valued keys to Any-valued values. Our
    database API is a bit different from the one presented in [1]. We perform
    reads and writes on entire objects, rather than on attributes within
    objects. We also remove the create, delete, copy, and exchange methods
    to keep things simple.

    [1]: https://scholar.google.com/scholar?cluster=14884990087876248723
    """
    def __init__(self, index) -> None:
        test = (
            ('a', 0),
            ('b', 0),
            ('c', 0),
            ('d', 0),
            ('e', 0),
            ('f', 0),
            ('g', 0),
            ('h', 0)
        )
        dbname = "table" + str(index)
        connect_str = "host={} dbname={} user={} password={}".format('localhost', dbname, 'linqi', '123')
        self.conn = psycopg2.connect(connect_str)
        with self.conn:
            cur = self.conn.cursor()

            cur.execute("DROP TABLE IF EXISTS test")
            cur.execute("CREATE TABLE test(name VARCHAR(255) PRIMARY KEY, price INT)")

            query = "INSERT INTO test (name, price) VALUES (%s, %s)"
            cur.executemany(query, test)

            self.conn.commit()
        #self.data: Dict[str, Any] = {'a':10}

    def __str__(self) -> str:
        return str(self.data)

    def write(self, name: str, val: Any) -> None:
        with self.conn:
            cur = self.conn.cursor()

            cur.execute("UPDATE test SET price=%s WHERE name=%s", (val, name))

            self.conn.commit()
        #self.data[name] = val

    def read(self, name: str) -> Any:
        with self.conn:
            cur = self.conn.cursor()

            cur.execute("SELECT * FROM test WHERE name=%s", name)
            self.conn.commit()
            row = cur.fetchone()
            return row[1]

        
        #assert name in self.data
        #return self.data[name]

class CachingDatabaseWrapper:
    """
    A CachingDatabaseWrapper provides the twrite/tread/tdelete interface
    described in [1]. A CachingDatabaseWrapper wrapper acts like a database,
    but writes are buffered in a local cache, and reads read from this cache
    (or the database, if the object being read hasn't been written).

    [1]: https://scholar.google.com/scholar?cluster=14884990087876248723
    """
    def __init__(self, db: Database) -> None:
        self.db = db
        self.copies: Dict[str, Any] = {}
        self.read_set: Set[str] = set()

    def write(self, name: str, val: Any) -> None:
        self.copies[name] = val

    def read(self, name: str) -> Any:
        self.read_set.add(name)
        if name in self.copies:
            return self.copies[name]
        else:
            return self.db.read(name)

    def commit(self) -> None:
        for k, v in self.copies.items():
            self.db.write(k, v)

    def get_write_set(self) -> Set[str]:
        return set(self.copies.keys())

    def get_read_set(self) -> Set[str]:
        return self.read_set

Transaction = Callable[[CachingDatabaseWrapper], None]
