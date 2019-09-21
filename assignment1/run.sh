MASTER_IP=`hostname -i`
PERSIST=0
NUM_PARTITIONS=0

if [ "$2" == "persist" ]; then
    PERSIST=1
fi

if [ "$#" -gt 2 ] && [ "$3" -gt "0" ]; then
    NUM_PARTITIONS=$3
fi

if [ $1 == "2" ]; then
    hdfs dfs -rm -r /CS744/assignment1/output.csv
    
    ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
        --executor-memory 32G \
        ~/CS744/assignment1/part2.py \
        hdfs://$MASTER_IP:9000/CS744/assignment1/export.csv \
        hdfs://$MASTER_IP:9000/CS744/assignment1/output.csv

elif [ $1 == "3" ]; then
    hdfs dfs -rm -r /CS744/assignment1/rank-BerkStan.txt

    ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
        --executor-memory 32G \
        ~/CS744/assignment1/pagerank.py \
        hdfs://$MASTER_IP:9000/CS744/assignment1/web-BerkStan.txt \
        hdfs://$MASTER_IP:9000/CS744/assignment1/rank-BerkStan.txt \
        --master_ip=$MASTER_IP \
        --persist=$PERSIST \
        --num_partitions=$NUM_PARTITIONS


elif [ $1 == "4" ]; then
    # hdfs dfs -rm -r /
    hdfs dfs -rm -r /enwiki-pages-articles/result.txt
    
    ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
        ~/CS744/assignment1/pagerank.py \
        hdfs://$MASTER_IP:9000/enwiki-pages-articles/*.xml-* \
        hdfs://$MASTER_IP:9000/enwiki-pages-articles/result.txt \
        --master_ip=$MASTER_IP \
        --persist=$PERSIST \
        --num_partitions=$NUM_PARTITIONS

fi
