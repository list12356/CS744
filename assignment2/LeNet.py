import json
import os
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers


# define the command line flags that can be sent
tf.app.flags.DEFINE_integer("task_index", 0, "Index of task with in the job.")
tf.app.flags.DEFINE_string("job_name", "worker", "either worker or ps")
tf.app.flags.DEFINE_string("deploy_mode", "single", "either single or cluster")
FLAGS = tf.app.flags.FLAGS

tf.logging.set_verbosity(tf.logging.DEBUG)
 
os.environ["TF_CONFIG"] = json.dumps({
    "cluster": {
        "worker": ["10.10.1.1:6222", "10.10.1.2:6222", "10.10.1.3:6222"],
    },
   "task": {"type": "worker", "index": FLAGS.task_index}
})

# clusterSpec_single = tf.train.ClusterSpec({
#     "worker" : [
#         "localhost:2222"
#     ]
# })
# 
# clusterSpec_cluster = tf.train.ClusterSpec({
#     "ps" : [
#         "10.10.1.1:2222"
#     ],
#     "worker" : [
#         "10.10.1.1:2223",
#         "10.10.1.2:2222"
#     ]
# })
# 
# clusterSpec_cluster2 = tf.train.ClusterSpec({
#     "ps" : [
#         "10.10.1.1:2222"
#     ],
#     "worker" : [
#         "10.10.1.1:2223",
#         "10.10.1.2:2222",
#         "10.10.1.3:2222",
#     ]
# })
# 
# clusterSpec = {
#     "single": clusterSpec_single,
#     "cluster": clusterSpec_cluster,
#     "cluster2": clusterSpec_cluster2
# }
# 
# clusterinfo = clusterSpec[FLAGS.deploy_mode]
# server = tf.train.Server(clusterinfo, job_name=FLAGS.job_name, task_index=FLAGS.task_index)

#put your code here
mirrored_strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()

with mirrored_strategy.scope():
    (x_train, y_train), (x_test, y_test) = keras.datasets.mnist.load_data()
    # Preprocess the data (these are Numpy arrays)
    x_train = x_train.reshape(60000, 28, 28, 1).astype('float32') / 255
    x_test = x_test.reshape(10000, 28, 28, 1).astype('float32') / 255
    
    y_train = y_train.astype('float32')
    y_test = y_test.astype('float32')
    
    # Reserve 10,000 samples for validation
    x_val = x_train[-10000:]
    y_val = y_train[-10000:]
    x_train = x_train[:-10000]
    y_train = y_train[:-10000]
    
    model = tf.keras.Sequential([
	    layers.Conv2D(filters=6, kernel_size=(3, 3), activation='relu', input_shape=(28, 28,1)),
	    layers.AveragePooling2D(),
	    layers.Conv2D(filters=16, kernel_size=(3, 3), activation='relu'),
	    layers.AveragePooling2D(),
	    layers.Flatten(),
	    layers.Dense(units=120, activation='relu'),
	    layers.Dense(units=84, activation='relu'),
	    layers.Dense(units=10, activation='softmax')
	])
    model.compile(optimizer=tf.keras.optimizers.Adam(0.01),
        loss=tf.keras.losses.CategoricalCrossentropy(),
        metrics=[tf.keras.metrics.CategoricalAccuracy()])
   
    print("Training")
    history = model.fit(x_train, y_train, epochs=10, batch_size=64*3)
    print('\nhistory dict:', history.history)
    print("evaluating")
    result = model.evaluate(x_test, y_test, batch_size=128*3)
    print("result:", result)
         

