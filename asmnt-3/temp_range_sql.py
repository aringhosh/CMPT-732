import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('example application').getOrCreate()
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 

inputs = sys.argv[1]
output = sys.argv[2]


def get_range(recordings):

    recordings.registerTempTable('Recordings')
    recordings = spark.sql("""
    SELECT * FROM Recordings WHERE QFlag IS NULL
    """)
    recordings.registerTempTable('Recordings')

    dfrange = spark.sql("""
    SELECT r1.DateTime, r1.StationID, (r1.DataValue-r2.DataValue) AS Range FROM
    (SELECT StationID, DateTime, Observation, DataValue FROM Recordings
     WHERE Observation='TMAX') r1
     JOIN
     (SELECT StationID, DateTime, Observation, DataValue FROM Recordings
     WHERE Observation='TMIN') r2
     ON (r1.StationID = r2.StationID AND r1.DateTime = r2.DateTime)
    """)
    dfrange.registerTempTable('RangeTable')

    df_maxrange = spark.sql("""
    SELECT DateTime, MAX(Range) AS MaxRange FROM RangeTable
    GROUP BY DateTime
    """)
    df_maxrange.registerTempTable('MaxRange')

    df_result = spark.sql("""
    SELECT t1.DateTime as DateTime, t1.StationID as StationID, t2.MaxRange as MaxRange FROM
    RangeTable t1
    JOIN MaxRange t2
    ON (t1.DateTime = t2.DateTime AND t1.Range = t2.MaxRange)
    ORDER BY DateTime
    """)

    return df_result


def main():
    # do things...
    _schema = StructType([
    StructField('StationID', StringType(), False),
    StructField('DateTime', LongType(), False),
    StructField('Observation', StringType(), False),
    StructField('DataValue', LongType(), False),
    StructField('MFlag', StringType(), True),
    StructField('QFlag', StringType(), True)
    ])

    df = spark.read.csv(inputs, schema= _schema)
       
    dfrange = get_range(df)

    outdata = dfrange.rdd.map(lambda r: str(r.DateTime)+' '+str(r.StationID)+' '+str(r.MaxRange))
    outdata.saveAsTextFile(output)
    
 
if __name__ == "__main__":
    main()