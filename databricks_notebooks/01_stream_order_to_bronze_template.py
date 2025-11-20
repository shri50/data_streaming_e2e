#Imports
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Storage location for catalog
# to get storage location got catalog >> settings >> catalog >> external location 
storage_location = 'abfss://unity-catalog-storage@dbstorageqcxubpqvntz3o.dfs.core.windows.net/1378876125019939'

# Create Unity Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS ecomdata MANAGED LOCATION '{storage_location}'")

# Create bronze layer schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ecomdata.bronze")

# Create silver layer schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ecomdata.silver")

# Create gold layer schema
spark.sql("CREATE SCHEMA IF NOT EXISTS ecomdata.gold")

# Create bronze layer volume for checkpoint
spark.sql("CREATE VOLUME IF NOT EXISTS ecomdata.bronze.bronze_volume")

# Create silver layer volume for checkpoint
spark.sql("CREATE VOLUME IF NOT EXISTS ecomdata.silver.silver_volume")

# Create gold layer volume for checkpoint
spark.sql("CREATE VOLUME IF NOT EXISTS ecomdata.gold.gold_volume")

#Schema of dataset
schema = StructType([
    StructField('order_id',StringType()),
    StructField('timestamp',StringType()),
    StructField('customer_id',StringType()),
    StructField('product_id',StringType()),
    StructField('category',StringType()),
    StructField('price',DoubleType()),
    StructField('quantity',IntegerType()),
    StructField('total_amount',DoubleType()),
    StructField('city',StringType()),
    StructField('state',StringType()),
    StructField('country',StringType()),
    StructField('latitude',StringType()),
    StructField('longitude',StringType()),
    StructField('delivery_status',StringType()),
])

# Azure Event Hub Configuration
event_hub_namespace = "<NAMESPACE_HOST_NAME>"
event_hub_name = "<EVENT_HUB_NAME>"
event_hub_conn_str = "<YOUR_EVENT_HUB_CONNECTION_STRING>"

eh_conf = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

#Stream from Event Hub
df_raw =  spark.readStream\
    .format('kafka')\
    .options(**eh_conf)\
    .load()


#Parse JSON from kafka stream
df_orders = (
    df_raw.selectExpr("Cast(value AS STRING) as json")
    .select(from_json("json",schema).alias("data"))
    .select("data.*")
)

df_orders.display()


df_orders.writeStream\
.format('delta')\
.outputMode("append")\
.option("checkpointLocation","/Volumes/ecomdata/bronze/bronze_volume")\
.toTable("ecomdata.bronze.bronze_data")