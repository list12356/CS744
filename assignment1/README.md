# CS744 Assignment 1

## Location of data sources

We make all the data under holder 
>hdfs://$MASTER_IP:9000/CS744/assignment1/

Where $MASTER_IP is resolved by `hostname -i` in our run.sh

## Part 2
To run part2, in the shell, simply type
`./run./sh`
The data source should be in 
>hdfs://\$MASTER_IP:9000/CS744/assignment1/export.csv


The result will be in 
>hdfs://\$MASTER_IP:9000/CS744/assignment1/output.csv

## Part 3

The run.sh have multiple commandline argument. The first argument choose which dataset to run, 3 means the BerkStan graph and 4 is the wikipedia graph. The next command line controls the persist RDD, 'persist' means make the graph RDD persistent during running the job. The last commandline will choose the partition number, if not specified, the default partition number will be used.

Example usage:
- run BerkStan graph with persist rdd and defualt partition number:
`./run.sh 3 persist`
- run wikipedia graph without persist rdd and 180 partition:
`./run.sh 4 nopersist 180`
