from pyspark import SparkConf
from pyspark.sql import SQLContext
import pyspark_cassandra
import sys

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('example code') \
		.set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
		.set('spark.dynamicAllocation.maxExecutors', 20)

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def df_for(keyspace, table, split_size=None):
	df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
	df.registerTempTable(table)
	return df

def rows_to_list(key_vals, key_col, list_col):
	#closure
	def listappend(lst, v):
		lst.append(v)
		return (lst)

	def listjoin(lst1, lst2):
		lst1.extend(lst2)
		return (lst1)
 
	key_val_rdd = key_vals.rdd.map(tuple)
	key_list_rdd = key_val_rdd.aggregateByKey([], listappend, listjoin)
	return sqlContext.createDataFrame(key_list_rdd, schema=[key_col, list_col])
	
def main():
	keyspace1 = sys.argv[1]
	keyspace2 = sys.argv[2]

	# get order, lineitem and part tables
	order = df_for(keyspace1, 'orders').cache()
	lineItem = df_for(keyspace1, 'lineitem').cache()
	part = df_for(keyspace1, 'part').cache()
	
	#join
	order_part = sqlContext.sql("SELECT a.orderkey, c.name " + \
		"FROM orders a JOIN lineitem b ON (a.orderkey = b.orderkey) " + \
		"JOIN part c on (b.partkey = c.partkey)")
	
	order_part2 = rows_to_list(order_part, 'orderkey', 'part_names')
	order_part2.registerTempTable('order_part')
	
	result = sqlContext.sql("SELECT a.*, b.part_names " + \
		"FROM orders a JOIN order_part b ON (a.orderkey = b.orderkey)")
	
	# result.show()

	#Save result to Cassandra
	result.rdd.map(lambda r: r.asDict()).saveToCassandra(keyspace2, 'orders_parts', \
		batch_size = 400)        
		
if __name__ == "__main__":
	main()