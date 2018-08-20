import sys
from pyspark.sql import SparkSession, functions, types
  
spark = SparkSession.builder.appName('example application').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 

inputs = sys.argv[1]
output = sys.argv[2]


def main():
    # do things...

    schema = types.StructType([
    types.StructField('_subreddit', types.StringType(), False),
    types.StructField('_score', types.FloatType(), False)])

    comments = spark.read.json(inputs, schema)
    comments.show()
    # averages = comments.select('subreddit', 'score').groupby('subreddit').avg()

    comments.createOrReplaceTempView('commentsView') # name of the table/view is commentsView
    averages = spark.sql("""
    SELECT _subreddit, AVG(_score)
    FROM commentsView
    GROUP BY _subreddit
""")
    averages.show()

    averages.write.save(output, format='json', mode='overwrite')
 
if __name__ == "__main__":
    main()