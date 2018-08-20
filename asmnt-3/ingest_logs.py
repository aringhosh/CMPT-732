import sys
from pyspark.sql import SparkSession, Row
import re, datetime
  
spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 

inputs = sys.argv[1]
output = sys.argv[2]


def parseline(line):
    linere = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    match = re.search(linere, line)
    if match:
        m = re.match(linere, line)
        host = m.group(1)
        date = datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
        path = m.group(3)
        bys = float(m.group(4))
        return (host, date, path, bys)
    return None

def return_me_row_from_parsed_line(d):
    (host, date, path, bys) = d
    row = Row(host = host, date =date, path = path, bys = bys)
    return (row)

def main():
    # do things...
    host_bytes = sc.textFile(inputs).map(lambda line: parseline(line)).filter(lambda x: x is not None)
    rows = host_bytes.map(return_me_row_from_parsed_line)
    df = spark.createDataFrame(rows)
    df.write.format('parquet').save(output)

 
if __name__ == "__main__":
    main()
    