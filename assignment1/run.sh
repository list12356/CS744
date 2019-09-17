~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit  --master local ~/CS744/assignment1/pagerank.py hdfs://172.31.45.228:9000/CS744/assignment1/web-BerkStan.txt hdfs://172.31.45.228:9000/CS744/assignment1/rank-BerkStan.txt 

# ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit  --master local ~/CS744/assignment1/pagerank.py hdfs://172.31.45.228:9000/CS744/assignment1/test.txt hdfs://172.31.45.228:9000//CS744/assignment1/test-out.txt  --debug=1
