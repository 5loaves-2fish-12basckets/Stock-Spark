# -*- coding: utf-8 -*-
"""
__author__  = '{Jimmy Yeh}'
__email__   = '{marrch30@gmail.com}'
"""

import pyspark
from pyspark.ml.feature import VectorAssembler, OneHotEncoder
from pyspark.ml.pipeline import Pipeline
from pyspark.sql import SparkSession

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


from sparkflow.graph_utils import build_graph, build_adam_config
from sparkflow.tensorflow_async import SparkAsyncDL

import os

import sys
# sys.path.append('src')

from datafunc import collect_data
from model import small_model

targets_tech = ['GOOGL','FB','MSFT','AAPL','INTC','TSM','ORCL',
            'IBM','NVDA','ADBE','TXN','AVGO','ACN','CRM','QCOM']
targets_etf = ['MS','VFH','TSM','IXG','RYF','UYG','DFNL','PSCF',
            'IAK','KBWP','CHIX','BDCS','KBWR','PFI','JHMF']
targets_rand = ['CY','KHC','AMAT','EBAY','URBN','ROST','ADI',
            'LRCX','RRGB', 'MCD', 'TER', 'ACGL', 'TSCO', 'TIVO', 'TSM']


datadir = 'data'

def main_collect():
    print('check data')
    if not os.path.exists(datadir):
        os.mkdir(datadir)
    if len(os.listdir(datadir)) == 0:
        print('     ## collecting all data##     ') 
        for targets in [targets_tech, targets_etf, targets_rand]:
            collect_data(datadir, targets)

if __name__ == '__main__':
    
    print('begin new task')
    print()
    print()

    main_collect()

    task = sys.argv[1] # tech, finn, food
    ckptdir = os.path.join('ckpt', task)

    targets = [targets_tech, targets_etf, targets_rand][['tech', 'etf', 'rand'].index(sys.argv[1])]
    
    conf = pyspark.SparkConf().setAppName("myAppName") #.set('spark.sql.catalogImplementation') # no hive
    sc = pyspark.SparkContext(conf=conf)
    sqlContext = pyspark.sql.SQLContext(sc)
    dic_list = list()
    min_count = 100000000
    tsm_dic = None

    #collect and process data from csvs of one target list
    print('==start to preprocess for %s =='%task)

    schema = StructType([
        StructField('timestamp', StringType(), True), 
        StructField('open', StringType(), True), 
        StructField('high', StringType(), True), 
        StructField('low', StringType(), True), 
        StructField('close', StringType(), True), 
        StructField('volume', StringType(), True)
        ])


    for symbol in targets:
        try:
            filepath = os.path.join(datadir, symbol+'.csv')
            raw_data = sc.textFile(filepath)
            rdd = raw_data.map(lambda x: x.split(",")).filter(lambda x: x[0]!='timestamp')
            df = sqlContext.createDataFrame(rdd, schema)
            df = df.drop('open').drop('high').drop('low').drop('volume')
            df = df.withColumn('close', df['close'].cast(DoubleType()))
            dic = list(map(lambda row: row.asDict(), df.collect()))
            dic_list.append(dic)
            if symbol == 'TSM':
                tsm_dic = dic
            if len(dic) < min_count:
                min_count = len(dic)
            print(symbol, 'done')

            
        except:
            print(symbol, 'has error')
            continue

    process_dic = list()
    for i in range(11,min_count):
        doc = list()
        doc.append(dic_list[0][i]['timestamp'])
        for dic in dic_list:
            for j in range(i-10,i):
                doc.append(dic[j]['close'])
        if tsm_dic[i-10]['close'] > tsm_dic[i-11]['close']:
            doc.append(0)
        else:
            doc.append(1)
        
        process_dic.append(doc)

    final_schema = [StructField('timestamp', StringType(), True)]
    for i in range(0,150):
        final_schema.append(StructField(str(i), DoubleType(), True))
    final_schema.append(StructField('result', IntegerType(), True))
    final_schema = StructType(final_schema)

    final_rdd = sc.parallelize(process_dic)
    final_df = sqlContext.createDataFrame(final_rdd, final_schema)

    print('== preprocess finished, final_df created ==')

    # create spark session and train with final_df
    spark = SparkSession.builder \
            .appName(task+'flow') \
            .getOrCreate()

    # sc.stop() ## stop?

    mg = build_graph(small_model)
    #Assemble and one hot encode
    va = VectorAssembler(inputCols=final_df.columns[1:151], outputCol='features')
    encoded = OneHotEncoder(inputCol='result', outputCol='labels', dropLast=False)
    adam_config = build_adam_config(learning_rate=0.001, beta1=0.9, beta2=0.999)
    
    spark_model = SparkAsyncDL(
        inputCol='features',
        tensorflowGraph=mg,
        tfInput='x:0',
        tfLabel='y:0',
        tfOutput='out:0',
        tfLearningRate=.001,
        iters=20,
        predictionCol='predicted',
        labelCol='labels',
        verbose=1,
        optimizerOptions=adam_config
    )
    
    ckptpath = os.path.join(ckptdir, task)
    print('save model in ckptpath')
    p = Pipeline(stages=[va, encoded, spark_model]).fit(final_df)
    p.write().overwrite().save(ckptpath)

    print('===task all done===')
