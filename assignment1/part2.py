from pyspark import SparkContext, SparkConf
import argparse

def main(input_path, output_path, master_ip):
    conf = SparkConf().setAppName("PageRank").setMaster("spark://{!s}:7077".format(master_ip))\
        .set('spark.executor.memory', '20g').set('spark.driver.memory', '20g')\
	.set('spark.eventLog.enabled', 'true')
    sc = SparkContext(conf=conf)
    distFile = sc.textFile(input_path)
    splitted = distFile.map(lambda s:s.split(','))
    temp = splitted.sortBy(lambda x: x[-1])
    result = temp.sortBy(lambda x: x[2])
    output = result.map(lambda s:','.join(s))
    output.saveAsTextFile(output_path)
    sc.stop()

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path')
    parser.add_argument('output_path')
    parser.add_argument('--master_ip', default="128.104.223.196")
    args = parser.parse_args()
    main(args.input_path, args.output_path, args.master_ip)
