from pyspark import SparkConf, SparkContext
import sys, json
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('reddit average')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
 
def dump_score(line):
	j = json.loads(line)
	yield (j['subreddit'], (1, j['score']) )

def add_pairs(x, y):
	return (x[0] + y[0], x[1] + y[1])
	
def output_format(kv):
	subreddit, (count, score) = kv
	return json.dumps((subreddit, score/count))

words = sc.textFile(inputs).flatMap(dump_score).reduceByKey(add_pairs).map(output_format)
words.coalesce(1).saveAsTextFile(output)