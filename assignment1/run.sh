HDFS_IP=172.31.12.172

hdfs dfs -rm -r /CS744/assignment1/rank-BerkStan.txt
~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit  --master local ~/CS744/assignment1/pagerank.py hdfs://$HDFS_IP:9000/CS744/assignment1/web-BerkStan.txt hdfs://$HDFS_IP:9000/CS744/assignment1/rank-BerkStan.txt  $HDFS_IP


# ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit  --master local ~/CS744/assignment1/pagerank.py hdfs://$HDFS_IP:9000/enwiki-pages-articles/*.xml-* hdfs://$HDFS_IP:9000/enwiki-pages-articles.rank
