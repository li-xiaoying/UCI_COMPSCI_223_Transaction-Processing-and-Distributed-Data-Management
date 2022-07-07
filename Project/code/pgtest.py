import psycopg2

#sudo apt-get install postgresql libpq-dev postgresql-client postgresql-client-common
#sudo -i -u postgres
#createuser linqi -P --interactive
#createdb table0
#createdb table1
#createdb table2
#pip install psycopg2


test = (
    (1, 'a', 0),
    (2, 'b', 0),
    (3, 'c', 0),
    (4, 'd', 0),
    (5, 'e', 0),
    (6, 'f', 0),
    (7, 'g', 0),
    (8, 'h', 0)
)
index = 0
dbname = "table" + str(index)
connect_str = "host={} dbname={} user={} password={}".format('localhost', dbname, 'linqi', '123')
#connect_str = "dbname='table0' user='linqi' host='localhost' " + \
#                  "password='123'"
conn = psycopg2.connect(connect_str)

with conn:

    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS test")
    cur.execute("CREATE TABLE test(id SERIAL PRIMARY KEY, name VARCHAR(255), price INT)")

    query = "INSERT INTO test (id, name, price) VALUES (%s, %s, %s)"
    cur.executemany(query, test)

    conn.commit()
    cur.execute("UPDATE test SET price=%s WHERE id=%s", (10, 1))
    name = 'a'
    cur.execute("SELECT * FROM test WHERE name=%s", name)

    row = cur.fetchone()

    print(f'{row[0]} {row[1]} {row[2]}')





