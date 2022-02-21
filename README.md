# Web-Server-Log-Analysis-with-PySpark
This example demonstrates parsing (including incorrectly formated strings) and analysis of web server log data . 

### History 
I started this project at 2015. That time, with Spark 1.4,  the RDD was the only structure to use. The main focus of the project was exploring the basic functionality of pypark and possibilities of exploratory analysis for distributed data.
The data used in 2015 [http://ita.ee.lbl.gov/html/contrib/Calgary-HTTP.html](http://ita.ee.lbl.gov/html/contrib/Calgary-HTTP.html) do not exist anymore. 

### Focus in 2022
I found many logs at [https://www.sec.gov/dera/data/edgar-log-file-data-set.html](https://www.sec.gov/dera/data/edgar-log-file-data-set.html)
Here I take the 2003 data, because the file is relatively small, about 3-4M. In 2017 the file size is about 300-400M.

Now the project has a new focus. This Jupyter Notebok and production scripts for batch processing serve as investigation steps. The main goal is stream processing of logs with Spark Streams and scheduling with Apache Airflow. 

### Why RDD und Spark Streams?
For historical reasons I decided to keep the project focused on RDDs and Spark Streams. They are suitable for unstructured data. The processing described here will bring the structure to data.

For initially structured data the better tools are DataFrames and StructuredStreams. For those I will find a new playground.

