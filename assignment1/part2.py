from pyspark import SparkContext, SparkConf
import argparse

def main(input_path, output_path):
    conf = SparkConf().setAppName("Sort").setMaster("spark://3.19.211.19:7077")
    sc = SparkContext(conf=conf)
    distFile = sc.textFile(input_path)
    splitted = distFile.map(lambda s:s.split(','))
    temp = splitted.sortBy(lambda x: x[-1])
    result = temp.sortBy(lambda x: x[0])
    output = result.map(lambda s:','.join(s))
    output.saveAsTextFile(output_path)

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path')
    parser.add_argument('output_path')
    args = parser.parse_args()
    main(args.input_path, args.output_path)
