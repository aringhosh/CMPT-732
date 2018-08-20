from pyspark import SparkConf
import pyspark_cassandra
import sys
from pyspark.sql import SparkSession

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('tpch_order_sql') \
		.set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
		.set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

spark = SparkSession.builder.appName('example application').getOrCreate()

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


######## functions

def df_for(keyspace, table, split_size=None):
	df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
	df.createOrReplaceTempView(table)
	return df

def append(x,y):
	(price1, part1) = x
	(price2, part2) = y
	price = price1
	part = part1 + ", " + part2
	return (price, part)

def formatOutput(x):
	(order, (price, key)) = x
	return ("Order #%s $%.2f: %s" % (order, price, key))	


## main logic #####

keyspace = sys.argv[1]
output = sys.argv[2]
orderkeys = sys.argv[3:]

orderList = "("
for orderkey in orderkeys:
	orderList = orderList + orderkey + ","
orderList = orderList[:-1] + ")"

#tables are cached here
orders = df_for(keyspace, 'orders').filter('orderkey in '+orderList).cache()
orders.registerTempTable('orders')
part = df_for(keyspace, 'part').cache()
lineitem = df_for(keyspace, 'lineitem').cache()

query = """
	SELECT o.orderkey, o.totalprice, p.name FROM orders o 
	JOIN lineitem l ON (o.orderkey = l.orderkey) 
	JOIN part p ON (l.partkey = p.partkey)
	"""

# we dont need to cache the following df since its only used once to covert to rdd
master_df = spark.sql(query)

# Result
outdata = master_df.rdd.map(lambda x: (x[0],(x[1],x[2]))) \
		.reduceByKey(append) \
		.map(formatOutput)

# print(outdata.collect())
outdata.coalesce(1).saveAsTextFile(output)
