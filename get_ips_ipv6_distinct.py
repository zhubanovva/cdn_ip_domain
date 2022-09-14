#import os
# Set spark environments
import os
import pandas as pd
os.environ["PYSPARK_DRIVER_PYTHON"] = '/usr/bin/python3.6'
os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3.6'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path /data-public/common_resources/ojdbc6-11.1.0.7.0.jar --jars /data-public/common_resources/ojdbc6-11.1.0.7.0.jar pyspark-shell '
import pyspark
from pyspark.sql import functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder \
    .config('spark.dynamicAllocation.enabled', 'true') \
    .appName('diana_test') \
    .enableHiveSupport() \
    .getOrCreate()

# import libraries     
import socket
from datetime import datetime

# variables
url_list = ['etc.example1.ru', 'example-2.audio.ru', 'mail.ru', 'ivi.ru', 'cdnvideo.ru', 'fghf.co'] 

ip_list=[]

# find ips of domains
for i in range(len(url_list)):
    a = socket.getaddrinfo(url_list[i],None)
    for result in a:
        ip_list.append([result[-1][0],url_list[i]])
        
ip_list = [list(x) for x in set(tuple(x) for x in ip_list)]

# get ips that are already exists for date and eliminate them
df = spark.sql(f"""select url,ip from owner_db.cdn_ip_domain where url_date = '{datetime.today().strftime('%Y-%m-%d')}'""").toPandas()

ip_list = [x for x in ip_list if x not in df.values.tolist()]

# insert to hive table
for i in range(len(ip_list)):
    spark.sq(f"""INSERT INTO TABLE owner_db.cdn_ip_domain values('{ip_list[i][0]}','{ip_list[i][1]}','{datetime.today().strftime('%Y-%m-%d')}')""")
