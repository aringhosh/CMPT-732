from pyspark import SparkConf
import pyspark_cassandra
import sys

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('tpch_orders_denorm') \
		.set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
		.set('spark.dynamicAllocation.maxExecutors', 20)

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


######## reading from cassandra

def rdd_for(keyspace, table, split_size=None):
	rdd = sc.cassandraTable(keyspace, table, split_size=split_size,\
		row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
	return (rdd)

def formatOutput(x):
	(order, price, part) = x
	return ("Order #%s $%.2f: %s" % (order, price, list_to_string(part)))

def list_to_string(aList):
	aString = ""
	for item in aList:
		aString = aString + str(item) + ", "
	aString = aString[:-2]
	return aString
	
def main():
	keyspace = sys.argv[1]
	output = sys.argv[2]
	orderkeys = sys.argv[3:]
		
	orderList = "("
	for orderkey in orderkeys:
		orderList = orderList + str(orderkey) + ","
	orderList = orderList[:-1] + ")"

	# get order, lineitem and part tables
	order = rdd_for(keyspace, 'orders_parts').where('orderkey in ' + orderList)   
	result = order.select('orderkey','totalprice','part_names')
	
	# Result
	outdata = result.map(lambda x: (x['orderkey'],x['totalprice'],list(x['part_names']))) \
		.map(formatOutput)
	
	outdata.coalesce(1).saveAsTextFile(output)
		
if __name__ == "__main__":
	main()