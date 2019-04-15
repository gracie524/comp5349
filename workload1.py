from pyspark import SparkContext
import argparse

if __name__ == "__main__":
    sc = SparkContext(appName="work1")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='~/assignment/')
    parser.add_argument("--output", help="the output path",
                        default='work1')
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output

    df = sc.textFile(input_path + "AllVideos_short.csv")
    alldata = df.map(lambda line:line.split(","))

    def filter_header(line):
        if line[0] == "video_id":
            return False
        else:
            return True

    data = alldata.filter(lambda line:filter_header(line))
    data.collect(5)
    num_id = data.map((lambda x:x[3],x[0])).distict().countByValue()
    num_id.collect(5)
    num_co = data.map((lambda x:x[0],x[11])).distict().countByValue()
    num_co.collect(5)
    cata_id = data.map((lambda x:x[0],x[3])).join(num_co)
    cata_id.collect(5)

    num_idco = cata_id.values().map((lambda x:x[0],x[1]))
    num_idco.take(5)

    sum_country = num_idco.reduceByKey((lambda x,y:x+y))
    cata_result = sum_country.join(num_id)
    finalresult = cata_result.mapValues((lambda x: x[0]/x[1]))
    finalresult.take(5)

    finalresult.saveAsTextFile(output_path)
