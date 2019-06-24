
from alpha_vantage.timeseries import TimeSeries
import csv
import time
import os
import pyspark
import sys
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import lit

from sparkflow.pipeline_util import PysparkPipelineWrapper
from pyspark.ml.pipeline import PipelineModel

from firebase import firebase

firebase = firebase.FirebaseApplication('https://cloud-computing-final-6bc36.firebaseio.com', None)


schema = StructType([StructField('timestamp', StringType(), True), StructField('open', StringType(), True), StructField('high', StringType(), True), StructField('low', StringType(), True), StructField('close', StringType(), True), StructField('volume', StringType(), True)])

datadir = 'data/demo'
modeldir = 'ckpt'

targets_tech = ['GOOGL','FB','MSFT','AAPL','INTC','TSM','ORCL',
            'IBM','NVDA','ADBE','TXN','AVGO','ACN','CRM','QCOM']
targets_etf = ['MS','VFH','TSM','IXG','RYF','UYG','DFNL','PSCF',
            'IAK','KBWP','CHIX','BDCS','KBWR','PFI','JHMF']
targets_rand = ['CY','KHC','AMAT','EBAY','URBN','ROST','ADI',
            'LRCX','RRGB', 'MCD', 'TER', 'ACGL', 'TSCO', 'TIVO', 'TSM']

cat_name = ['tech','etf','rand']

if __name__ == '__main__':
    conf = pyspark.SparkConf().setAppName(sys.argv[2])
    sc = pyspark.SparkContext(conf=conf)
    sqlContext = pyspark.sql.SQLContext(sc)

    if len(os.listdir(datadir)) == 0:
        print("     ## Collecting demo data ##     ")
        for targets in [targets_tech, targets_etf, targets_rand]:
            collect_intraday_data(datadir, targets)

        schema = StructType([
            StructField('timestamp', StringType(), True),
            StructField('open', StringType(), True),
            StructField('high', StringType(), True),
            StructField('low', StringType(), True),
            StructField('close', StringType(), True),
            StructField('volume', StringType(), True)
            ])
        counter = 0
        for target in [targets_tech,targets_etf,targets_rand]:
            tsm_dic = None
            min_count = 1000000000
            dic_list = list()
            for symbol in target:
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
            final_df.write.csv(datadir+cat_name[counter]+'.csv')

            counter += 1

    target = [targets_tech, targets_etf, targets_rand][['tech', 'etf', 'rand'].index(sys.argv[1])]
    tsm_dic = None
    min_count = 1000000000
    dic_list = list()
    for symbol in target:
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

    # final_dict = map(lambda row: row.asDict(), final_df.collect())
    # final_dict = list(final_dict)

    model_dir = os.path.join(modeldir,sys.argv[1])

    p = PysparkPipelineWrapper.unwrap(PipelineModel.load(model_dir))
    predictions = p.transform(final_df)
    predictions_dict = list(map(lambda row: row.asDict(), predictions.collect()))

    for i in range(len(predictions_dict)-1, -1, -1):
        upload_data = {
            'date':predictions_dict[i]['timestamp'],
            'predict':predictions_dict[i]['predicted'],
            'result':predictions_dict[i]['result'],
            'price':tsm_dic[i]['close']
        }
        firebase.post('/'+sys.argv[1],data = upload_data,params = {'print': 'pretty'})
        print(upload_data)
        time.sleep(60)

