from sparkflow.graph_utils import build_graph
from sparkflow.tensorflow_async import SparkAsyncDL
import tensorflow as tf
from pyspark.ml.feature import VectorAssembler, OneHotEncoder
from pyspark.ml.pipeline import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

#simple tensorflow network
def small_model():
    x = tf.placeholder(tf.float32, shape=[None, 149], name='x')
    y = tf.placeholder(tf.float32, shape=[None, 3], name='y')
    layer1 = tf.layers.dense(x, 256, activation=tf.nn.relu)
    layer2 = tf.layers.dense(layer1, 256, activation=tf.nn.relu)
    out = tf.layers.dense(layer2, 3)
    z = tf.argmax(out, 1, name='out')
    loss = tf.losses.softmax_cross_entropy(y, out)
    return loss

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("examples") \
        .getOrCreate()
    
    # df = spark.read.option("inferSchema", "true").csv('mnist_train.csv', inferSchema =True,header=True)

    df = spark.sql("SELECT \
     * \
     FROM r07943149_training_data2")
    mg = build_graph(small_model)
    #Assemble and one hot encode
    va = VectorAssembler(inputCols=df.columns[1:150], outputCol='features')
    encoded = OneHotEncoder(inputCol='result', outputCol='labels', dropLast=False)
    
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
        verbose=1
    )
    
    p = Pipeline(stages=[va, encoded, spark_model]).fit(df)
    p.write().overwrite().save("tfmodel")


# r07943149_training_data1
# r07943149_training_data2
# r07943149_training_data3