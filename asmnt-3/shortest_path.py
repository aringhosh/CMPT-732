import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.context import SQLContext

spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

inputs = sys.argv[1]
output = sys.argv[2]
source_node = sys.argv[3]
dest_node = sys.argv[4]

def get_graphedges(line):
	list1 = line.split(':')
	if list1[1] == '':
		return None
	else:
		s = list1[0]

	list2 = list1[1].split(' ')
	list2 = filter(None, list2)
	results = []
	for d in list2:
		results.append((s, d))
	return (results)

def get_source_for_node(val, resulted_df, l_output):
	result_row = resulted_df.where(resulted_df.node == val).select("source")
	result = result_row.rdd.flatMap(list).first()
	l_output.append(result)
	if(result != source_node):
		return(get_source_for_node(result, resulted_df, l_output))
	else:
		return(l_output)

def write_output(path):
	with open(output +'out.txt', 'w') as f:
		for node in path:
			f.write(node +"\n")

# main logic 
def main():
	if(source_node == dest_node):
		print('source is same as destination')
		sys.exit()


	textinput = sc.textFile(inputs + 'links-simple-sorted.txt')
	graphedges_rdd = textinput.map(lambda line: get_graphedges(line)).\
							filter(lambda x: x is not None).flatMap(lambda x: x)
	graphedges = graphedges_rdd.toDF(['source', 'destination']).cache()
	print('\nconstructed edge graph df from given input')
	graphedges.show()
	KnownRow = Row('node', 'source', 'distance')

	schema = StructType([
		StructField('node', StringType(), False),
		StructField('source', StringType(), False),
		StructField('distance', IntegerType(), False),
		])

	newRow = KnownRow(source_node,-1, 0) # source -1 means node don't have any src
	finalOut = sqlContext.createDataFrame([newRow], schema=schema).cache()

	# we process inter_df on each loop
	# we first find out what sources its nodes goes into
	# store the result and re process using the same logic
	# we are running these for 6 times max
	# but since we are checking the equality at the start of the loop
	# this runs 7 times, the 7th time, it just quits once it don't see any match
	inter_df = finalOut
	for i in range(7):
		
		print('loop: ',i)
		finalOut.show()
		finalOut.write.save(output + '/iter' + str(i), format='json')

		if(inter_df.filter(inter_df.node == dest_node).count() > 0):
			print('match found')
			break
		elif(i == 6): 
			#the distance cannot be greater than 6
			print('no match within 6 steps')
			finalOut = sqlContext.createDataFrame(sc.emptyRDD(), schema)
			break

		cond = [inter_df['node'] == graphedges['source']]
		df_result = graphedges.join(inter_df, cond, 'inner').\
				select(graphedges['destination'].alias('node'), graphedges['source'],\
				(inter_df['distance'] +1).alias('distance')).cache()

		# no rows means it has no children, 
		# so we no need to continue
		if(df_result.rdd.isEmpty()):
			print('no match found')
			finalOut = sqlContext.createDataFrame(sc.emptyRDD(), schema)
			break
		
		# only include uncommon rows from df_result to final df
		# also we store these rows and pass them for next loop
		df_result = df_result.join(finalOut, ['node'], "leftanti").cache()
		inter_df = df_result

		# also, append our newly founded rows here
		# in the final resulted df
		finalOut = finalOut.union(df_result)


	#output
	print('final - output')
	finalOut.show()
	if len (finalOut.take(1)) != 0 : #not empty
			l = get_source_for_node(dest_node, finalOut, [dest_node])
			l.reverse()
			print(l)
			write_output(l)


if __name__ == "__main__":
	main()
