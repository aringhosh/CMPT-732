import sys
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf, max


spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+

inputs = sys.argv[1]
output = sys.argv[2]

def extract_hour_from_filename(fname):
	return (fname[-18: -7])
def main():
	
	schema = StructType([
    StructField('LANG', StringType(), False),
    StructField('TITLE', StringType(), False),
    StructField('VIEWS', LongType(), False),
    StructField('BYTES', LongType(), False)
    ])

	df = spark.read.csv(inputs, schema=schema, sep = " ").withColumn('filename', functions.input_file_name())
	# df.show()
	hour_udf = udf(extract_hour_from_filename, StringType())
	df = df.select('LANG', 'TITLE', 'VIEWS', 'BYTES', hour_udf("filename").alias("HOUR"))

	df = df.where(df.LANG == 'en')
	df = df.where((~ df.TITLE.startswith('Main_Page')))
	df = df.where((~ df.TITLE.startswith('Special:')))
	# df.show()

	df_max_views = df.groupby('HOUR').agg(max("VIEWS").alias('MAX_VIEW'))

	cond = [df['HOUR'] == df_max_views['HOUR'], df['VIEWS'] == df_max_views['MAX_VIEW']]
	result = df.join(df_max_views, cond, 'inner').select(df['HOUR'], df['TITLE'], df['VIEWS']).sort(df['HOUR'])

	# result.show()

	result.write.csv(output)

if __name__ == "__main__":
    main()


