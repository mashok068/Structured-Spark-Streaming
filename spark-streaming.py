"""
-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
  Project Retail Analysis using Apache Spark Streaming using Spark 2.3.0 on Hadoop 2.6
  Problem Statement :  To Calculate Key Performance Indicators (KPIs) for an e-commerce company, RetailCorp Inc. We have been provided real-time sales data ofa company across the globe. The data contains information related to the invoices of orders placed by customers all around the world. This data has to be read from a Kafka server. Perform Aggregate Transformations. After the Transformations, The KPI's have to be displayed on the Console and Written to Json File.

Project : By Malathi Ashok
Date    : 09/08/2021
-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
"""

# System dependencies for CDH
import os
import sys

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

####################################################################################
# Utility function for calculating total number of items present in a single order
####################################################################################
def get_total_item_count(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']

   return total_count

####################################################################################
# Utility function for calculating total cost of an order per invoice
####################################################################################
def get_total_cost(type, items):
   total_cost = 0
   for item in items:
       total_cost = total_cost + (item['unit_price'] * item['quantity'])
   if type == 'ORDER':
       return total_cost
   else:
       return (-total_cost)

####################################################################################
# Utility function to figure out the order type
####################################################################################
def get_is_order(type):
   if type == 'ORDER':
       return 1
   else: 
       return 0

####################################################################################
# Utility function to figure out the return type
####################################################################################
def get_is_return(type):
   if type == 'RETURN':
       return 1
   else: 
       return 0

####################################################################################
# Utility function to figure out the average transaction size
####################################################################################
def get_avg_trans_size(total_sale_amount, total_orders, total_returns):
   if (total_orders+total_returns > 0):
       return total_sale_amount/float(total_orders + total_returns)

   else:
       return 0.0


####################################################################################
# Utility function to figure out the average rate of return 
####################################################################################
def get_avg_rate_of_return(total_orders, total_returns):
   if (total_orders+total_returns > 0):
       return total_returns/float(total_orders+total_returns)
   else:
       return 0.0
       
if __name__ == "__main__":

   # Validate Command line arguments
   if len(sys.argv) != 4:
       print("Usage: spark-submit spark-streaming.py <hostname> <port> <topic>")
       exit(-1)

   host = sys.argv[1]
   port = sys.argv[2]
   topic = sys.argv[3]


   # Read Input from kafka
   spark = SparkSession \
       .builder \
       .appName("OrderAnalyzer") \
       .getOrCreate()
   spark.sparkContext.setLogLevel("ERROR")

   # Read Input from the kafka server as a Json String
   orderRaw = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", host + ":" + port) \
       .option("startingOffsets", "latest") \
       .option("subscribe", topic) \
       .load()

   # Define schema of a single order
   jsonSchema = StructType() \
       .add("invoice_no", LongType()) \
       .add("country", StringType()) \
       .add("timestamp", TimestampType()) \
       .add("type", StringType()) \
       .add("items", ArrayType(StructType([
       StructField("SKU", StringType()),
       StructField("title", StringType()),
       StructField("unit_price", FloatType()),
       StructField("quantity", IntegerType()), 
   ])))

   # Extract individual Columns from the Json into OrderStream DataStream
   orderStream = orderRaw.select(from_json(col("value").cast("string"), jsonSchema).alias("data")).select("data.*")
   
   ##################################################################################################
   # Define the UDFs with the utility functions
   ##################################################################################################
   add_total_item_count = udf(get_total_item_count, IntegerType())
   add_total_order_cost = udf(get_total_cost, FloatType())
   add_is_order = udf(get_is_order, IntegerType())
   add_is_return = udf(get_is_return, IntegerType())
   add_avg_trans_size = udf(get_avg_trans_size, FloatType())
   add_rate_of_return = udf(get_avg_rate_of_return, FloatType())

   #################################################################################################
   # Calculate additional columns
   #################################################################################################
   expandedOrderStream = orderStream \
       .withColumn("total_cost", add_total_order_cost(orderStream.type, orderStream.items)) \
       .withColumn("total_items", add_total_item_count(orderStream.items)) \
       .withColumn("is_order", add_is_order(orderStream.type)) \
       .withColumn("is_return", add_is_return(orderStream.type))

   ################################################################################################
   # Write the summarized input values on Console
   ################################################################################################
   query = expandedOrderStream \
       .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()
   
   # ************ Transform the input stream by performing the group aggregation ***************
   #              using window function with a window interval of 1 minute and a tumbling
   # *********** window of 1 minute allowing for a late arrival of 1 minute   ******************

   aggStreamByTimeForConsole = expandedOrderStream \
       .withWatermark("timestamp", "1 minute") \
       .groupBy(window("timestamp", "1 minute", "1 minute")) \
       .agg(round(sum("total_cost"), 2).alias("total_sale_volume"), \
            count("invoice_no").alias("OPM"), \
            sum("is_order").alias("total_orders"), \
	    sum("is_return").alias("total_returned_orders")) \
       .orderBy(asc("window")) \
       .select("window", "OPM", "total_sale_volume", "total_orders", "total_returned_orders") 
               
   kpiStreamByTimeForConsole = aggStreamByTimeForConsole \
      .withColumn("rate_of_return", round(add_rate_of_return(aggStreamByTimeForConsole.total_orders, aggStreamByTimeForConsole.total_returned_orders),2)) \
      .withColumn("average_transaction_size", round(add_avg_trans_size(aggStreamByTimeForConsole.total_sale_volume, aggStreamByTimeForConsole.total_orders, aggStreamByTimeForConsole.total_returned_orders),2)) 

   ###############################################################################################
   #  Write the the kpi's of the time query to the console - This need not be outputed to Console
   #  as per Rubrics
   ###############################################################################################
   queryByTimeConsoleDF =kpiStreamByTimeForConsole \
      .select("window", "OPM", "total_sale_volume", "average_transaction_size", "rate_of_return") \
      .writeStream \
      .format("console") \
      .outputMode("complete") \
      .trigger(processingTime="1 minute") \
      .option("truncate", "false") \
      .start() 

   aggStreamByTimeForJson = expandedOrderStream \
       .withWatermark("timestamp", "1 minute") \
       .groupBy(window("timestamp", "1 minute", "1 minute")) \
       .agg(round(sum("total_cost"), 2).alias("total_sale_volume"), \
            count("invoice_no").alias("OPM"), \
            sum("is_order").alias("total_orders"), \
	    sum("is_return").alias("total_returned_orders")) \
       .select("window", "OPM", "total_sale_volume", "total_orders", "total_returned_orders") 
               
   kpiStreamByTimeForJson = aggStreamByTimeForJson \
      .withColumn("rate_of_return", round(add_rate_of_return(aggStreamByTimeForJson.total_orders, aggStreamByTimeForJson.total_returned_orders),2)) \
      .withColumn("average_transaction_size", round(add_avg_trans_size(aggStreamByTimeForJson.total_sale_volume, aggStreamByTimeForJson.total_orders, aggStreamByTimeForJson.total_returned_orders),2)) 

   ##########################################################i#######################################
   #  Write the the kpi's of the time query as json file - as per Rubrics
   ##################################################################################################
   queryByTimeJsonDF =kpiStreamByTimeForJson \
      .select("window", "OPM", "total_sale_volume", "average_transaction_size", "rate_of_return") \
      .writeStream \
      .format("json") \
      .outputMode("append") \
      .trigger(processingTime="1 minute") \
      .option("checkpointLocation", "/user/root/kafkarealtime/time_kpi") \
      .option("path", "/user/root/kafkarealtime/time_kpi") \
      .option("truncate", "false") \
      .start() 


   aggStreamByCountryForConsole = expandedOrderStream \
       .withWatermark("timestamp", "1 minute") \
       .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
       .agg(round(sum("total_cost"), 2).alias("total_sale_volume"), \
            count("invoice_no").alias("OPM"), \
            sum("is_order").alias("total_orders"), \
	    sum("is_return").alias("total_returned_orders")) \
       .orderBy(asc("window"),"country") \
       .select("window", "country", "OPM", "total_sale_volume", "total_orders", "total_returned_orders") 
               
   kpiStreamByCountryForConsole = aggStreamByCountryForConsole \
      .withColumn("rate_of_return", round(add_rate_of_return(aggStreamByCountryForConsole.total_orders, aggStreamByCountryForConsole.total_returned_orders),2)) \

   ####################################################################################################
   #  Write the the kpi's of the Country query to the console - This need not be outputted to Console
   #  as per Rubrics
   ####################################################################################################
   queryByCountryConsoleDF =kpiStreamByCountryForConsole \
      .select("window", "country", "OPM", "total_sale_volume", "rate_of_return") \
      .writeStream \
      .format("console") \
      .outputMode("complete") \
      .trigger(processingTime="1 minute") \
      .option("truncate", "false") \
      .start() 

   aggStreamByCountryForJson = expandedOrderStream \
       .withWatermark("timestamp", "1 minute") \
       .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
       .agg(round(sum("total_cost"), 2).alias("total_sale_volume"), \
            count("invoice_no").alias("OPM"), \
            sum("is_order").alias("total_orders"), \
	    sum("is_return").alias("total_returned_orders")) \
       .select("window", "country", "OPM", "total_sale_volume", "total_orders", "total_returned_orders") 
               
   kpiStreamByCountryForJson = aggStreamByCountryForJson \
      .withColumn("rate_of_return", round(add_rate_of_return(aggStreamByCountryForJson.total_orders, aggStreamByCountryForJson.total_returned_orders),2)) \

   ######################################################################################################
   #  Write the the kpi's of the Country query as Json File
   ######################################################################################################
   queryByCountryJsonDF =kpiStreamByCountryForJson \
      .select("window", "country", "OPM", "total_sale_volume", "rate_of_return") \
      .writeStream \
      .format("json") \
      .outputMode("append") \
      .trigger(processingTime="1 minute") \
      .option("checkpointLocation", "/user/root/kafkarealtime/country_kpi") \
      .option("path", "/user/root/kafkarealtime/country_kpi") \
      .option("truncate", "false") \
      .start()
   
   # Run the process until a termination request comes in
   query.awaitTermination()
   queryByTimeConsoleDF.awaitTermination()
   queryByTimeJsonDF.awaitTermination()
   queryByCountryConsoleDF.awaitTermination()
   queryByCountryJsonDF.awaitTermination()
