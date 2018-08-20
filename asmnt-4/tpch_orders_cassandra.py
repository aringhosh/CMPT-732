from pyspark import SparkConf
import pyspark_cassandra
import sys
# from pyspark.sql import SparkSession

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('tpch_order_cassandra') \
		.set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
		.set('spark.dynamicAllocation.maxExecutors', 20)

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

# spark = SparkSession.builder.appName('example application').getOrCreate()

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


######## reading from cassandra
	
def rdd_for(keyspace, table, split_size=None):
	rdd = sc.cassandraTable(keyspace, table, split_size=split_size,
		row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
	return rdd
	
def formatOutput(x):
	(order, price, key) = x
	return ("Order #%s $%.2f: %s" % (order, price, key))


def main():
	keyspace = sys.argv[1]
	output = sys.argv[2]
	orderkeys = sys.argv[3:]
		
	orderList = "("
	for orderkey in orderkeys:
		orderList = orderList + str(orderkey) + ","
	orderList = orderList[:-1] + ")"

	# get order, lineitem and part tables, no need to cache
	order = rdd_for(keyspace, 'orders').where('orderkey in ' + orderList) \
		.select('orderkey','totalprice')
	dict_order = order.collect() # .collect() is safe since the number of parts is probably small.
	
	lineitem = rdd_for(keyspace, 'lineitem').where('orderkey in ' + orderList) \
		.select('orderkey','partkey')
	dict_lineitem = lineitem.collect() # .collect() is safe since the number of parts is probably small.
		
	partList = "("
	for aLineItem in dict_lineitem:
		partList = partList + str(aLineItem.get('partkey')) + ","
	partList = partList[:-1] + ")"
		
	part = rdd_for(keyspace, 'part').where('partkey in ' + partList) \
		.select('partkey','name')
	dict_part = part.collect() # .collect() is safe since the number of parts is probably small.
	
	#join
	for aLineItem in dict_lineitem:
		partkey = aLineItem.get('partkey')
		for aPart in dict_part:
			if aPart.get('partkey') == partkey:
				aLineItem['name'] = aPart.get('name')
				break
		
	for aOrder in dict_order:
		orderkey = aOrder.get('orderkey')
		part_names = ""
		for aLineItem in dict_lineitem:
			if aLineItem.get('orderkey') == orderkey:
				part_names = part_names + aLineItem.get('name') + ", "
		aOrder['part_names'] = part_names[:-2]
	

	# Result
	result = sc.parallelize(dict_order)
	outdata = result.map(lambda x: (x['orderkey'],x['totalprice'],x['part_names'])) \
		.map(formatOutput)

	outdata.coalesce(1).saveAsTextFile(output)
		
if __name__ == "__main__":
	main()
