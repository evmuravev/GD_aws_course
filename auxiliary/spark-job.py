from pyspark.sql import functions as F
from pyspark.sql import types as T
import boto3
from pyspark.sql.session import SparkSession
import sys


bucket = sys.argv[1]
key = sys.argv[2]
dbtable = sys.argv[3]
path = 's3://' + bucket + '/' + key


def write_to_dynamodb(row):
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table(dbtable)
    table.put_item(Item={'ip': row['user_ip']})


spark = SparkSession.builder.getOrCreate()
df = spark.read.json(path)

result = (df.withColumn('ts', F.col('ts').astype(T.TimestampType()))
            .withColumn('ts', F.date_trunc(timestamp='ts', format='second')) 
            .groupBy('user_ip', 'ts')
            .count()
            .groupBy('user_ip')
            .agg(F.avg(F.col('count')).alias('avg'))
            .where(F.col('avg') > 5)
            .select('user_ip'))

result.show(5)

for row in result.collect():
    write_to_dynamodb(row)

