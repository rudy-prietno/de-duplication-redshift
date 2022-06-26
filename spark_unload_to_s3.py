import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf

runtimes = []
startTime = time.time()

# be careful, look for the spark jar file in aws that matches the spark version your setup
spark = SparkSession.\
        builder.\
        appName(args.spark_name).\
        master(f"local[{args.core}]").\
        config('spark.jars', '/home/ubuntu/lib/aws-java-sdk-bundle-1.11.901.jar,/home/ubuntu/lib/hadoop-aws-3.3.1.jar,/home/ubuntu/lib/postgresql-42.3.3.jar').\
        config("spark.ui.port", "4050").\
        config("spark.executor.cores", "48").\
        config("spark.executor.memory", "28g").\
        config("spark.driver.memory", f"{args.ram}g").\
        config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
        config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY").\
        config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED").\
        config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED").\
        config("fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com").\
        config('fs.s3a.access.key', f"{args.access_key}").\
        config('fs.s3a.secret.key', f"{args.secret_key}").\
        getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

runtime = round(time.time()-startTime, 2)
runtimes.append(('create_session', runtime))
print('Running time:', runtime)


# read data
# jdbc:postgresql://localhost:5432/database_example
# s3a://bucket_name/object_path_name/ingestion_date/

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", f"{args.connection_db}")\
    .option("dbtable", f"{args.table_name}") \
    .option("user", f"{args.users}") \
    .option("password", f"{args.pass}") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# jdbcDF.printSchema()
DF_write = jdbcDF.select('*').collect()

# if you need change data type
# Change column type
# DF_new = DF_write.withColumn("myColumn", df["myColumn"].cast(IntegerType()))
# DF_new.printSchema()

DF_write.write.parquet(F'{args.s3path}/{args.date}/',mode="overwrite")
# DF_new.write.parquet(F'{args.s3path}/{args.date}/',mode="overwrite")

spark.stop()
