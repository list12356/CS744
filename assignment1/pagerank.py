from pyspark import SparkContext, SparkConf
from operator import add
import argparse

def contribute(rank, edges):
    num_nbr = len(edges)
    for x in edges:
        yield (x, rank/num_nbr)

def filter_articles(article_list):
    if len(article_list) != 2:
        return False
    
    for article in article_list:
        if ':' in article and not article.startswith('Category:'):
            return False
    
    return True


def main(input_path, output_path, debug=0, iter_num=10, master_ip='172.31.12.172', \
          persist=False, num_partitions=None):
    conf = SparkConf().setAppName("PageRank-{!s}-PartNum-{!s}"\
        .format(input_path.split('/')[-1], num_partitions if num_partitions else "default"))\
        .setMaster("spark://{!s}:7077".format(master_ip))\
        .set('spark.executor.memory', '20g').set('spark.driver.memory', '20g')\
	.set('spark.eventLog.enabled', 'true')
    sc = SparkContext(conf=conf)
    distFile = sc.textFile(input_path, num_partitions)
    splitted = distFile.map(lambda s: [x.rstrip().lower() for x in s.split('\t')])
    splitted = splitted.filter(filter_articles)
    out_edges = splitted.map(lambda s: (s[0], s[1])).groupByKey(numPartitions=num_partitions)
    graph = out_edges.mapValues(list)
    rank = out_edges.mapValues(lambda x: 1.0)

    if persist:
        graph.persist()

    for i in range(iter_num):
        temp = rank.join(graph)
        temp = temp.flatMap(lambda x: contribute(x[1][0], x[1][1])).\
            reduceByKey(add, numPartitions=num_partitions)
        rank = temp.mapValues(lambda x: x*0.85 + 0.15)
        
    if persist:
        graph.unpersist()

    rank.saveAsTextFile(output_path)

    if debug:
        with open("/home/shitao/debug_pagerank.out", 'w+') as debug_file:
            for key, x in rank.collectAsMap().items():
                debug_file.write("{!s} : {!s}\n".format(key, x))

    sc.stop()


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path')
    parser.add_argument('output_path')
    parser.add_argument('--debug', default=0, type=int)
    parser.add_argument('--master_ip', default='128.104.223.196')
    parser.add_argument('--persist', default=0, type=int)
    parser.add_argument('--num_partitions', default=0, type=int)
    args = parser.parse_args()
    if args.num_partitions <= 0:
        args.num_partitions = None 
    main(args.input_path, args.output_path, debug=args.debug, master_ip=args.master_ip, \
        persist=bool(args.persist), num_partitions=args.num_partitions)
