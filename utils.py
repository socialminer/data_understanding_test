from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

'''
R:
'''
def get_session() -> SparkSession:
    return SparkSession.builder \
        .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0') \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "90320") \
        .config("spark.cassandra.auth.username", "athenas") \
        .config("spark.cassandra.auth.password", "OIIUUYHB76$") \
        .config('mapreduce.input.fileinputformat.input.dir.recursive', 'true') \
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport().getOrCreate()


'''
R:
'''
def persisted(df: DataFrame, dir_str):
    try:
        df.repartition(1).write.mode('overwrite').csv(dir_str, sep='|', header=True)
        return True
    except Exception as e:
        return False
