from pyspark import SparkConf, SparkContext
import sys
import re
import math
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('correlate logs 1')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def parseline(line):
    linere = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    match = re.search(linere, line)
    if match:
        m = re.match(linere, line)
        host = m.group(1)
        bys = float(m.group(4))
        return (host, (1, bys))
    return None

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def getRComponents(t):
	host, (n, bytes) = t
	return ('all_hosts', (bytes, n, pow(bytes, 2), pow(n, 2), bytes*n, 1))
	
def calculateR(comp):
	(host, (sum_x, sum_y, sum_x2, sum_y2, sum_xy, n)) = comp
	r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_x2 - pow(sum_x, 2)) * math.sqrt(n*sum_y2 - pow(sum_y, 2)))
	return r


host_bytes = sc.textFile(inputs).map(lambda line: parseline(line)).filter(lambda x: x is not None)
host_bytes = host_bytes.reduceByKey(add_tuples)
host_bytes = host_bytes.map(getRComponents)
host_bytes = host_bytes.reduceByKey(add_tuples)

_r = host_bytes.map(calculateR).collect()

## Print on command line
#print('r = ', _r[0])
#print('r^2 = ', pow(_r[0] , 2) )

## output on file 
str1 = 'r = '+ str(_r[0])
str2 = 'r^2 = '+ str(pow(_r[0],2))
sc.parallelize([str1, str2]).coalesce(1).saveAsTextFile(output)

