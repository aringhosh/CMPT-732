from pyspark import SparkConf, SparkContext
import sys, json
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('relative score')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+
 
def dump_score(line):
	yield(json.loads(line))

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def getAverage(kv):
	subreddit, (score, count) = kv
	return (subreddit, score/count)

def formatOutputData(data):
	k, (c, avg) = data
	return (c['score']/avg, c['author'])

comments = sc.textFile(inputs).flatMap(dump_score).cache()
commentbysub = comments.map(lambda c: (c['subreddit'], c))

c_score = comments.map(lambda c: (c['subreddit'], (c['score'], 1)))
c_scoresum = c_score.reduceByKey(add_tuples) 

c_avgscore = c_scoresum.map(getAverage).filter(lambda avg: avg[1] > 0)
reddits = commentbysub.join(c_avgscore)

output_data = reddits.map(formatOutputData).sortBy(lambda t: t[0], False)

output_data.saveAsTextFile(output)
