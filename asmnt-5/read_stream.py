from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName('Read_Stream').getOrCreate()

messages = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
    .option('subscribe', 'xy-10').load()

messages.createOrReplaceTempView("tbl_m")
msg_df = spark.sql("""SELECT CAST(value as string) as col1 from tbl_m""")
split_c = functions.split(msg_df.col1, ' ')

msg_df = msg_df.withColumn('x', split_c.getItem(0))
msg_df = msg_df.withColumn('y', split_c.getItem(1))
msg_df = msg_df.withColumn('xy',msg_df.x*msg_df.y)
msg_df = msg_df.withColumn('xsq',msg_df.x*msg_df.x)
msg_df.createOrReplaceTempView("updated_tbl")

req_df = spark.sql("""Select SUM(x) as sumx, SUM(y) as sumy, SUM(xy) as sumxy, SUM(xsq) as sumxsq, COUNT(*) as countdf from updated_tbl""")
req_df.createOrReplaceTempView("t1")
slope_df = spark.sql("""SELECT (sumxy-(1/countdf*(sumx*sumy)))/(sumxsq -(1/countdf*(sumx*sumx))) as slope, sumx, sumy , countdf from t1""")
slope_df.createOrReplaceTempView("slope_table")
intercept_df = spark.sql("""SELECT slope, (sumy-slope*sumx)/countdf as intercept from slope_table""")

stream = intercept_df.writeStream.format('console').outputMode('update').start()
stream.awaitTermination(120)