from pyspark import SparkContext, SparkConf
from operator import add
import argparse

def contribute(rank, edges):
    num_nbr = len(edges)
    for x in edges:
        yield (x, rank/num_nbr)

def main(input_path, output_path, debug=0, iter_num=10,\
            spark_ip='172.31.12.172', directory=False):
    conf = SparkConf().setAppName("PageRank").setMaster("spark://{!s}:7077".format(spark_ip))
    sc = SparkContext(conf=conf)
    distFile = sc.textFile(input_path)
    splitted = distFile.map(lambda s: [x.rstrip() for x in s.split('\t')])
    splitted = splitted.filter(lambda s: len(s) == 2)
    out_edges = splitted.map(lambda s: (s[0], s[1])).groupByKey()
    graph = out_edges.mapValues(list)
    rank = out_edges.mapValues(lambda x: 1.0)

    # rank.persist()
    graph.persist()
    for i in range(iter_num):
        temp = rank.join(graph)
        temp = temp.flatMap(lambda x: contribute(x[1][0], x[1][1])).reduceByKey(add)
        rank = temp.mapValues(lambda x: x*0.85 + 0.15)
        
    
    rank.saveAsTextFile(output_path)

    # with open("/home/shitao/debug_pagerank.out", 'w+') as debug_file:
    #    for key, x in rank.collectAsMap().items():
    #        debug_file.write("{!s} : {!s}\n".format(key, x))


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path')
    parser.add_argument('output_path')
    parser.add_argument('--debug', default=0)
    parser.add_argument('spark_ip')
    args = parser.parse_args()
    main(args.input_path, args.output_path, debug=args.debug, spark_ip=args.spark_ip)
