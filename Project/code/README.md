# 223-project

1. install postgreSQL
2. creat user xx
3. creat three tables：table0， table1， table2

#sudo apt-get install postgresql libpq-dev postgresql-client postgresql-client-common

#sudo -i -u postgres

#createuser xx -P --interactive

#createdb table0

#createdb table1

#createdb table2

#pip install psycopg2

4. edit database.py to connect with three tables
5. run client node by: python client.py -id 0 -nm 10
6. run node0 by:python nodetest.py -i 0
7. run node1 by:python nodetest.py -i 1
8. run node2 by:python nodetest.py -i 2

9. run test.py to test 6 cases by:python test.py

