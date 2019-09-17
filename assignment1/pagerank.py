from pyspark import SparkContext, SparkConf
from operator import add
import argparse

def contribute(rank, edges):
    num_nbr = len(edges)
    for x in edges:
        yield (x, rank/num_nbr)

def main(input_path, output_path, debug=0, iter_num=10):
    conf = SparkConf().setAppName("PageRank").setMaster("spark://3.19.211.19:7077")
    sc = SparkContext(conf=conf)
    distFile = sc.textFile(input_path)
    splitted = distFile.map(lambda s: s.decode('utf-8').split())
    out_edges = splitted.map(lambda s: (s[0], s[1])).groupByKey()
    graph = out_edges.mapValues(list)
    rank = out_edges.mapValues(lambda x: 1.0)

    # rank.persist()
    for i in range(iter_num):
        temp = rank.join(graph)
        temp = temp.flatMap(lambda x: contribute(x[1][0], x[1][1])).reduceByKey(add)
        rank = temp.mapValues(lambda x: x*0.85 + 0.15)
        
    
    # rank.saveAsTextFile(output_path)

    with open("/home/shitao/debug_pagerank.out", 'w+') as debug_file:
        for key, x in rank.collectAsMap().items():
            debug_file.write("{!s} : {!s}\n".format(key, x))


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path')
    parser.add_argument('output_path')
    parser.add_argument('--debug', default=0)
    args = parser.parse_args()
    main(args.input_path, args.output_path, debug=args.debug)
