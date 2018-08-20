from pyspark import SparkConf
import pyspark_cassandra
import sys, datetime
import math

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('correlate_logs_cas') \
		.set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


######## reading from cassandra

def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size,\
    	row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
    return (rdd)

def get_key_values_from_rowDict(row):
	host = row['host']
	bys = row['bytes']
	return (host, (1, bys))

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def getRComponents(t):
	host, (n, bytes) = t
	return ('all_hosts', (bytes, n, pow(bytes, 2), pow(n, 2), bytes*n, 1))
	
def calculateR(comp):
	(host, (sum_x, sum_y, sum_x2, sum_y2, sum_xy, n)) = comp
	r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_x2 - pow(sum_x, 2)) * math.sqrt(n*sum_y2 - pow(sum_y, 2)))
	return r

### main logic
keyspace = sys.argv[1]
table = sys.argv[2]

host_bytes = rdd_for(keyspace, table)
host_bytes = host_bytes.map(get_key_values_from_rowDict)
host_bytes = host_bytes.reduceByKey(add_tuples)
host_bytes = host_bytes.map(getRComponents)
host_bytes = host_bytes.reduceByKey(add_tuples)
_r = host_bytes.map(calculateR).collect()

str1 = 'r = '+ str(_r[0])
str2 = 'r^2 = '+ str(pow(_r[0],2))

print(str1)
print(str2)
