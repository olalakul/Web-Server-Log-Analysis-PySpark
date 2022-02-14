import logging

# python related
import argparse
from datetime import datetime
import os
import io 
import requests
import zipfile

# pyspark related
from pyspark.sql import SparkSession

# own modules
import pattern_EDGAR as pE
import utils

def parse_arguments():
    parser = argparse.ArgumentParser(
                      description='Parse arguments for log anylases',
                      epilog='End parsing arguments')
    parser.add_argument("--dateoflogs", type=lambda s: datetime.strptime(s, '%Y%m%d').date(),
                        default=datetime.today().date(), 
                        help='date to parse from 2003 till June 2017\
                        in the format "%Y%m%d", for example "20030209"')
    parser.add_argument("--data_directory", type=utils.dir_path, default='./data/',
                        help='directory to temporary store logging data')
    parser.add_argument("--output_directory", type=utils.dir_path, default='./output/',
                        help='directory to temporary store logging data')
    
    args = parser.parse_args()
    return args


def get_data_into_row_RDD(date, data_directory):
    year = date.year
    Qtr = (date.month-1)//3+1; print(Qtr)
    csv_file = os.path.join(args.data_directory, f'log{date:%Y%m%d}.csv') 
    logger.info(f'csv_file:  {csv_file:s}')
    #
    if not os.path.exists(csv_file):
        zip_file_url = f'http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/{year}/Qtr{Qtr:d}/log{date:%Y%m%d}.zip'
        logger.info(f'zip_file_url:  {zip_file_url:s}')
        r = requests.get(zip_file_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(data_directory)
    else:
        pass  # the file is already their 
    #
    logFileRDD = sc.textFile(csv_file, 4).cache()
    header = logFileRDD.first() #extract header
    if logger.isEnabledFor(logging.DEBUG):
        logFileRDD.take(5)
    #
    # TODO: find a better way to get rid of the first row
    logFileRDD = logFileRDD.filter(lambda row: row != header)    
    #
    return logFileRDD



def my_main():
    logFileRDD = get_data_into_row_RDD(args.dateoflogs, args.data_directory)
    
    logger.info('parsing logs')
    parsedLogsRDD1 = logFileRDD.map(pE.parseApacheLogLine).cache()
    if logger.isEnabledFor(logging.DEBUG):
        parsedLogsRDD1.take(3)

    logger.info('accessing failed logs')
    accessLogsRDD1, failedLogsRDD1 = utils.access_fail_logs(parsedLogsRDD1)
    logger.info(f'{failedLogsRDD1.count():d} logs failed to parse')
    if logger.isEnabledFor(logging.INFO):
        failedLogsRDD1.take(20)
        
    accessLogsRDD1.saveAsTextFile(
        os.path.join(args.output_directory, f'parsed_log{args.dateoflogs:%Y%m%d}.csv'))
        


if __name__ == '__main__':

    args=parse_arguments()
    
    spark = SparkSession.builder.appName('parse_logs').getOrCreate()
    sc = spark.sparkContext

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(name)-12s %(funcName)s line_%(lineno)d %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    py4j_logger = logging.getLogger("py4j")
    py4j_logger.setLevel(logging.ERROR)

    my_main()

    logger.info('DONE processing parse_logs.py')



