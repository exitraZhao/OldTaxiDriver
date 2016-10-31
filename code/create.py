
import cql
import cassandra
from cassandra.cluster import Cluster
from thrift.transport import TSocket


def create():
    '''create keyspace and table'''

    try:
        print "connecting"
        cluster = Cluster(['114.215.156.15',])
        session = cluster.connect()
        session.execute("create KEYSPACE hw2_1452739 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        session.execute("use hw2_1452739;")
        session.execute("create table track(TRIP_ID text primary key , CALL_TYPE text, ORIGIN_CALL int, ORIGIN_STAND int, TAXI_ID int, TIMESTAMP int, DAYTYPE text, MISSING_DATA Boolean, POLYLINE text);")

    except Exception,exce:
        print "!!!"
        print exce




if __name__ == '__main__':
    create()