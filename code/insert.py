import csv
import cql
import cassandra
from cassandra.cluster import Cluster
import datetime

def load():

    try:
        csvTable = file('../file/test.csv','rb')
        table = csv.reader(csvTable)
        cluster = Cluster(['114.215.156.15',])
        session = cluster.connect()
        session.execute("use hw2_1452739;")
        num = 0
        for each in table:
            if (num != 0):
                transStrinfToInt(each)
                each[5] = str(datetime.datetime.utcfromtimestamp(each[5]))
                if each[7] == 'False':
                    each[7] = False
                else:
                    each[7] = True

                cqlOpreation = "insert into test (TRIP_ID,CALL_TYPE,ORIGIN_CALL,ORIGIN_STAND,TAXI_ID,TIMESTAMP,DAY_TYPE,MISSING_DATA,POLYLINE) " \
                            "values ('{}','{}',{},{},{},'{}','{}',{},'{}')".format(each[0], each[1], each[2], each[3],
                                                                                   each[4], each[5], each[6], each[7],
                                                                                   each[8])
                print cqlOpreation
                session.execute(cqlOpreation)
            num += 1
    except Exception,exce:
        print '!!!!!'
        print exce

def transStrinfToInt(array):
    for i in range(2,6):
        if array[i] != 'NA':
            array[i] = int(array[i])
        else:
            array[i] = 'null'



if __name__ == '__main__':
    load()