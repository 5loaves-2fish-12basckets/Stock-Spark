# -*- coding: utf-8 -*-
"""
__author__  = '{Jimmy Yeh}'
__email__   = '{marrch30@gmail.com}'
"""

import tensorflow as tf

def small_model():
    x = tf.placeholder(tf.float64, shape=[None, 150], name='x')
    y = tf.placeholder(tf.float32, shape=[None, 2], name='y')
    layer1 = tf.layers.dense(x, 256, activation=tf.nn.relu)
    layer2 = tf.layers.dense(layer1, 256, activation=tf.nn.relu)
    out = tf.layers.dense(layer2, 2)
    z = tf.argmax(out, 1, name='out')
    loss = tf.losses.softmax_cross_entropy(y, out)
    return loss



