In Structured-Spark-Streaming, we are tasked with computing various Key Performance Indicators (KPIs) for an e-commerce company, 
RetailCorp Inc. We have been provided real-time sales data of the 
company across the globe through a Kafka topic. The data contains information related to the invoices of orders placed by customers all around the world. 

We will perform the following tasks in this project:
  Reading the sales data from the Kafka server
  Preprocessing the data to calculate additional derived columns such as total_cost etc
  Calculating the time-based KPIs and time and country-based KPIs
  Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis

