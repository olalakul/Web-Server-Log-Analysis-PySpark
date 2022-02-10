import re
from pyspark.sql import Row
from datetime import datetime

import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(name)-12s %(funcName)s line_%(lineno)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

py4j_logger = logging.getLogger("py4j")
py4j_logger.setLevel(logging.ERROR)


re_ip = '\d{1,3}\.\d{1,3}\.\d{1,3}\.\w{3}'
re_date = '\d{4}\-\d{2}\-\d{2}'
re_time = '\d{2}:\d{2}:\d{2}'
re_zone = '\d+\.?\d*'
re_cik = '\d+\.?\d*'
re_accession = '[\w\-]+'
re_filename = '[\w\-\.]+'
re_code = '\d+\.?\d*'
re_size = '\d*\.?\d*'
re_idx = '[01]\.?0?' 
re_norefer = '[01]\.?0?'
re_noagent = '[01]\.?0?' 
re_find = '1?[0-9]\.?0?'
re_crawler = '[01]\.?0?' 
re_browser = '[a-z]{0,3}'
#
LOG_PATTERN_EDGAR = f'^({re_ip:s}),({re_date:s}),({re_time:s}),({re_zone:s}),({re_cik:s}),\
({re_accession:s}),({re_filename:s}),({re_code:s}),({re_size:s}),({re_idx:s}),\
({re_norefer:s}),({re_noagent:s}),({re_find:s}),({re_crawler:s}),({re_browser:s})$'
#
logger.info(f'LOG_PATTERN_EDGAR:  {LOG_PATTERN_EDGAR:s}')
pattern=re.compile(LOG_PATTERN_EDGAR)


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Inputs: 
        logline (str): a line of text in the Apache Common Log format
    Outputs:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(pattern, logline)
    if match is None:   # failed match
        # TODO: define new level HARD_DEBUG = 5 and use it here
        #logger.debug(f'parsing failed   {logline:s}')
        return (logline, 0)
    
    size_field = match.group(9)
    if size_field:
        size = float(match.group(9))
    else:    
        size = float(0)
        
    browser_field = match.group(15)
    if browser_field:
        browser = browser_field
    else:
        browser = 'not_found'
    
    parsed_row = Row(
        ip          = match.group(1),
        date = datetime.strptime(match.group(2), "%Y-%m-%d").date(),
        time       = datetime.strptime(match.group(3), "%H:%M:%S").time(),
        zone        = match.group(4),
        cik      = match.group(5),
        accesion      = match.group(6),
        filename = match.group(7),
        response_code = int(float(match.group(8))),
        content_size  = size,
        idx = bool(match.group(10)),
        browser = browser
    )
    #logger.debug(f'parsed   {logline:s}')
    
    return (parsed_row, 1)


def distinct_responsecodes_browsers(accessLogsRDD):
    """
    Prints distinct values for  response codes and browsers
    Inputs:
        accessLogsRDD
    """
    ResponseCodesRDD = accessLogsRDD.map(lambda log: log.response_code)
    uniqueResponseCodesRDD = ResponseCodesRDD.distinct()
    uniqueResponseCodes = sorted(uniqueResponseCodesRDD.collect()) 
    logger.debug(f'Response codes are {str(uniqueResponseCodes):s}')
    BrowserRDD = accessLogsRDD.map(lambda log: log.browser)
    uniqueBrowsersRDD = BrowserRDD.distinct()
    uniqueBrowsers = sorted(uniqueBrowsersRDD.collect())
    logger.debug(f'Browsers are {str(uniqueBrowsers):s}')
    return uniqueResponseCodes, uniqueBrowsers
