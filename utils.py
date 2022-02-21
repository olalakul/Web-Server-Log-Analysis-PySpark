import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(name)-12s %(funcName)s line_%(lineno)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

py4j_logger = logging.getLogger("py4j")
py4j_logger.setLevel(logging.ERROR)

import os

    
def file_path(string):
    """ check if the path to the file"""
    logger.debug(string)
    if os.path.isfile(string):
        return string
    else:
        raise FileNotFoundError(string)

def dir_path(string):
    """ check if the path to the file"""
    logger.debug(string)
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)    

def dir_or_new_path(string):
    """ check if the path to the file"""
    logger.debug(string)
    if os.path.isdir(string):
        return string
    elif not os.path.exists(string):
            os.makedirs(string)  
    else:
        raise NotADirectoryError(string)    


    
    

def access_fail_logs(parsed_logs, info_from_batch_processing=False):
    """ Read and parse log file, print a 20-sample of failed log-lines
    Inputs:
        parsed_logs (RDD): an RDD obtained via parseApacheLogLine(...)
    Outputs:
        tuple of RDDs: access_logs, failed_logs
    """
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                  )

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0])
                  )
    
    if info_from_batch_processing:
        failed_logs_count = failed_logs.count()
        if failed_logs_count > 0:
           logger.info(f'Number of invalid logline: {failed_logs.count():d}')
           for line in failed_logs.take(20):
               logger.info(f'Invalid logline: {line:s}')

        logger.info(f'Read {parsed_logs.count():d} lines, successfully parsed { access_logs.count():d} lines, \
                     failed to parse {failed_logs.count():d} lines')

        if logger.isEnabledFor(logging.INFO):
            print('Examples of the successfully parsed access logs ')
            access_logs.pprint(4)
            print('Examples of failed logs')
            failed_logs.pprint(4)
    
    return access_logs, failed_logs