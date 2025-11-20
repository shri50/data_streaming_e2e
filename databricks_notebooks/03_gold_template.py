from pyspark.sql.functions import *
silver_path = "ecomdata.silver.silver_data"
gold_path = "ecomdata.gold.gold_data"
#Read from silver
df_silver=spark.readStream\
    .format("delta")\
    .load(silver_path)

#Aggregation: Total sales and total items sold per state per minute
df_gold=(
    df_silver
    .withWatermark("timestamp","1 minute")
    .groupby(
        window("timestamp","1 minute"),
        "state"
    )
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_items")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "state",
        "total_sales",
        "total_items"
    )
)

# Write to gold layer

df_gold.writeStream\
.format("delta")\
.outputMode("append")\
.option("checkpointLocation","/Volumes/ecomdata/gold/gold_volume")\
.toTable(gold_path)
