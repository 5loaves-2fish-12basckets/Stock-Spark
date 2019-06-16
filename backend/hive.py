from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

# SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")

if __name__ == '__main__':
    print('init')
    sconf = SparkConf().setAppName("r06921105py")
    sc = SparkContext(conf = sconf)
    spark = SparkSession.builder \
            .appName('examplehive') \
            .enableHiveSupport() \
            .getOrCreate()
    hiveContext = HiveContext(sc)
    print('start')
    print('hive')
    table_list = hiveContext.sql("SHOW TABLES")

    print()
    print('spark')
    print(spark.sql('SHOW TABLES'))
    print('done')



spark.sql("SELECT \
* \
FROM r07943149_training_data2")