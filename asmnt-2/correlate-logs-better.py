from pyspark import SparkConf, SparkContext
import sys
import re
import math
import operator
 
inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('correlate logs improved')
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

def getXYComponents(t):
	host, (x, y) = t
	return (x, y)

def x_minus_xbar_square(x):
     return pow((x[0]-x_mean), 2)

def y_minus_ybar_square(y):
     return pow((y[1]-y_mean), 2)

def xDiffTimesYDiff(xy):
    x = xy[0]
    y = xy[1]
    return (x - x_mean) * (y - y_mean)

host_bytes = sc.textFile(inputs).map(lambda line: parseline(line)).filter(lambda x: x is not None)
host_bytes = host_bytes.reduceByKey(add_tuples) # google.com, 5,2003; apple.com, 6, 20112
_pairs = host_bytes.map(getXYComponents) # 5, 2003; 6, 20112

sum_x = _pairs.map(lambda x: x[0]).reduce(operator.add)
x_mean = sum_x / len(_pairs.collect())
sum_y = _pairs.map(lambda x: x[1]).reduce(operator.add)
y_mean = sum_y / len(_pairs.collect())

num = _pairs.map(xDiffTimesYDiff).reduce(operator.add)
denom1 = math.sqrt(_pairs.map(x_minus_xbar_square).reduce(operator.add))
denom2 = math.sqrt(_pairs.map(y_minus_ybar_square).reduce(operator.add))
r = num/(denom1*denom2)
r2 = pow(r, 2)

## Print on command line
#print('r = ', r)
#print('r^2 = ', r2)

## output on file 
str1 = 'r = '+ str(r)
str2 = 'r^2 = '+ str(r2)
sc.parallelize([str1, str2]).coalesce(1).saveAsTextFile(output)
