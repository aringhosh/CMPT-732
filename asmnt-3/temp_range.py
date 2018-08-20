import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, sum, max

spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 

inputs = sys.argv[1]
output = sys.argv[2]



def main():
    # do things...
    schema = types.StructType([
    types.StructField('ID', types.StringType(), False),
    types.StructField('DATE', types.LongType(), False),
    types.StructField('TYPE', types.StringType(), False),
    types.StructField('VALUE1', types.LongType(), False),
    types.StructField('MFlag', types.StringType(), True),
    types.StructField('QFlag', types.StringType(), True)
    ])

    t = spark.read.csv(inputs, schema=schema)
    t = t.where(col("QFlag").isNull())

    p_max = t.filter((col("TYPE") == "TMAX")).select(t.ID, t.DATE, t.VALUE1.alias('MAX'))
    p_min = t.filter((col("TYPE") == "TMIN")).select(t.ID, t.DATE, t.VALUE1.alias('MIN'))

    cond = [p_max['ID'] == p_min['ID'], p_max['DATE'] == p_min['DATE'] ]
    df_result = p_max.join(p_min, cond, 'inner').select(p_max['DATE'], p_max['ID'], (p_max['MAX']-p_min['MIN']).alias('Range'))

    max_table = df_result.groupby('DATE').agg(max("Range").alias('MaxRange'))

    cond2 = [df_result['DATE'] == max_table['DATE'], df_result['Range'] == max_table['MaxRange'] ]
    df_result = df_result.join(max_table, cond2, 'inner').select(df_result['DATE'], df_result['ID'], max_table['MaxRange'].alias('RANGE'))
    df_result = df_result.sort(df_result['DATE'])
    # df_result.show()

    outdata = df_result.rdd.map(lambda r: str(r.DATE)+' '+str(r.ID)+' '+str(r.RANGE))
    outdata.saveAsTextFile(output)
    
 
if __name__ == "__main__":
    main()