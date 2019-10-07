#!/bin/bash

export TF_LOG_DIR="/tmp/lenet/mnist/logs"
# mkdir $TF_LOG_DIR
# cluster_utils.sh has helper function to start process on all VMs
# it contains definition for start_cluster and terminate_cluster
source cluster_utils.sh
start_cluster LeNet.py cluster

tensorboard --logdir $TF_LOG_DIR
# defined in cluster_utils.sh to terminate the cluster
# terminate_cluster
