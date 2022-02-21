import logging

# python related
import argparse
from datetime import datetime
import os
import io 
import requests
import sqlite3
import zipfile

# pyspark related
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# own modules
import pattern_EDGAR as pE
import utils

def parse_arguments():
    parser = argparse.ArgumentParser(
                      description='Parse arguments for log anylases',
                      epilog='End parsing arguments')
    parser.add_argument("--host", type=str, default='localhost',
                        help='Listening for a client at host')
    parser.add_argument("--port", type=int, default=8890,
                        help='Listening for a client at port')
    parser.add_argument("--output_directory", type=utils.dir_or_new_path, default='./output/',
                        help='directory to store output data')
    
    args = parser.parse_args()
    return args


def my_main():
    
    # Create a DStream that will connect to hostname:port, like localhost:8090
    logFileRDD = ssc.socketTextStream(args.host, args.port)
    
    logger.info('parsing logs')
    parsedLogsRDD1 = logFileRDD.map(pE.parseApacheLogLine).cache()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('Print the first 3 elements of each RDD generated in this DStream to the console')
        parsedLogsRDD1.pprint(3)

    logger.info('accessing failed logs')
    accessLogsRDD1, failedLogsRDD1 = utils.access_fail_logs(parsedLogsRDD1)
    if logger.isEnabledFor(logging.INFO):
        print('Examples of the successfully parsed access logs')
        accessLogsRDD1.pprint(3)
        print('Examples of failed logs')
        failedLogsRDD1.pprint(3)

    access_filename = os.path.join(args.output_directory, f'access_logs_from_socket.csv')
    accessLogsRDD1.saveAsTextFiles(access_filename)
    
    failed_filename = os.path.join(args.output_directory, f'failed_logs_from_socket.csv')
    failedLogsRDD1.saveAsTextFiles(failed_filename)
    
    ssc.start()  
    # Start the computation
    ssc.awaitTermination(120)  # Wait 60 sec for the computation to terminate 
    ssc.stop() 
    
        
        
if __name__ == '__main__':

    args = parse_arguments()
    
    spark = SparkSession.builder.appName('parse_logs').getOrCreate()
    sc = spark.sparkContext
    #sc = SparkContext("local[2]", "NetworkWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 30)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(name)-12s %(funcName)s line_%(lineno)d %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    py4j_logger = logging.getLogger("py4j")
    py4j_logger.setLevel(logging.ERROR)

    my_main()

    logger.info('DONE processing parse_logs_from_socket.py')
