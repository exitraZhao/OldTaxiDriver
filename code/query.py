import cql
import cassandra
from cassandra.cluster import Cluster

CORNER_LEFT = [-8.605,41.143]
CORNER_RIGHT = [-8.6045,41.149]

def query():
    try:

        con = cql.connect(host='114.215.156.15', keyspace="hw2_1452739", cql_version='3.0.0')
        cursor = con.cursor()
        cursor.execute("select * from test")
        list = cursor.result

        #cluster = Cluster(['114.215.156.15', ])
        #session = cluster.connect()
        #session.execute("use hw2_1452739;")
        #cqlString = "SELECT * from test"
        # #list = session.execute(cqlString)
        answerArray = analysis(list)

        for each in answerArray:
            '''print with out enter'''
            print each,

    except Exception, exce:
        print '!!!!!'
        print exce

def analysis(list):
    '''analysis data and return the target ID'''
    targetArray = []
    for each in list:
        rawList = transStringToRawList(each[6])
        for rawPoint in rawList:
            if rawPoint != "" and ifPointRight(rawPoint.split(',')):
                targetArray.append(each[0].value)
                break
    return targetArray

def transStringToRawList(str):
    '''split string to arrays'''
    rawList = str.value[1:-1].replace(']','').replace('[','/').split('/')
    return  rawList

def ifPointRight(point):
    '''check the Correctness of points'''
    if float(point[0]) < CORNER_LEFT[0] \
            or float(point[0]) > CORNER_RIGHT[0] \
            or float(point[1]) < CORNER_LEFT[1] \
            or float(point[1]) > CORNER_RIGHT[1]:
        return False
    return True
if __name__ == '__main__':
    query()