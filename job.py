from datetime import timedelta
from sys import argv

from pyspark.sql import Window
from pyspark.sql.functions import *

from constants import *
from default_args import DefaultArgs
from utils import get_session, persisted


class Job(DefaultArgs):

    def __init__(self, environment, dt) -> None:
        super().__init__(environment, dt)
        self.spark = get_session()
        self.behavior_path = f's3a://raw-bucket/behaviors'
        self.dir_value = f's3a://staged-bucket/behavior/output/{self.dt}'

    def __amount_sales_path(self, customer):
        default_path = f's3a://curated-bucket/campaign_results'
        prefix_path = f'{self.dt}/events/{customer}'
        return '{}/{}/*.csv'.format(default_path, prefix_path)

    def __get_min_proc(self):
        return self.dt - timedelta(days=7)

    def runner(self):
        '''
        R:
        '''
        mutable_df = self.spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table='consolidated_person', keyspace='sminer').load()

        for customer in CUSTOMERS:
            '''
            R: 
            '''
            mutable_people = mutable_df.filter(col('customerid') == customer) \
                .select('personid').distinct().collect()

            '''
            R:
            '''
            people_values = [s[0] for s in mutable_people]

            '''
            R:
            '''
            behavior_df = self.spark.read.parquet(self.behavior_path)

            '''
            R:
            '''
            between_min = self.__get_min_proc()

            '''
            R:
            '''
            behavior_df = behavior_df.where(col('date').between(str(between_min), str(self.dt)))

            '''
            R:
            '''
            behavior_df = behavior_df.withColumn('is_communication', when(col('personid').isin(people_values), True)) \
                .filter(col('is_communication').isNotNull()).drop('is_communication')

            '''
            R:
            '''
            df_behavior_grpy = behavior_df.groupBy('CustomerId', 'PersonId').count()

            '''
            R:
            '''
            pre_sales = self.spark.read.csv(self.__amount_sales_path(customer), header=True,
                                            sep='\t').filter(col('Type').isin(TYPES))

            '''
            R:
            '''
            ranking = Window.partitionBy('customerid', 'PersonId').orderBy('createdate')

            '''
            R:
            '''
            df_join_func = df_behavior_grpy.join(pre_sales, 'PersonId', 'outer')\
                .withColumn('row_number_col', row_number().over(ranking)) \
                .filter(col('row_number_col') == 1).drop('row_number_col')

            '''
            R:
            '''
            df_del = self.spark.read.csv(
                f's3a://staged/behavior/output/{self.dt - timedelta(days=1)}').withColumnRenamed('personid',
                                                                                                 'personiddelta')

            '''
            R:
            '''
            df_dist_delta = df_join_func.join(df_del, ['customerid', 'personid'], 'left').filter(
                col('personiddelta').isNull())

            '''
            R:
            '''
            persisted(df_dist_delta, self.dir_value)


if __name__ == '__main__':
    env = argv[1]
    date_proc = argv[2]
    job = Job(env, date_proc)
    job.runner()
