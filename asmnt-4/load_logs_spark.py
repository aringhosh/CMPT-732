from pyspark import SparkConf
import pyspark_cassandra
import sys, uuid, datetime
import re

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('load_logs') \
		.set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def parseline(line):
	linere = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
	match = re.search(linere, line)
	if match:
		d = {}
		m = re.match(linere, line)
		
		host = m.group(1)
		date = datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
		path = m.group(3)
		bys = float(m.group(4))

		d['host'] = host
		d['datetime'] = date
		d['path'] = path
		d['bytes'] = bys
		d['uid'] = uuid.uuid1()
		return (d)

	return None

###### insert logic here
inputs = sys.argv[1]
keyspace = sys.argv[2]
output_table = sys.argv[3]

# repartition = 100
host_bytes = sc.textFile(inputs, 100).map(lambda line: parseline(line)).filter(lambda x: x is not None)
host_bytes.saveToCassandra(keyspace, output_table, batch_size=300, parallelism_level= 100)
