from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# =====================================================
# INITIALIZE SPARK
# =====================================================

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# =====================================================
# READ SILVER DATA
# =====================================================

df = spark.read.parquet(
    "s3://food-delivery-logistics-data-project/silver/deliveries/"
)

# =====================================================
# GOLD 1 - DELIVERY KPIS
# =====================================================

gold_delivery_kpis = df.agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time"),
    round(avg("delivery_person_ratings"), 2).alias("avg_driver_rating"),
    countDistinct("delivery_person_id").alias("total_drivers")
)

gold_delivery_kpis.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_delivery_kpis/"
    )

# =====================================================
# GOLD 2 - CITY PERFORMANCE
# =====================================================

gold_city_performance = df.groupBy(
    "city"
).agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time"),
    round(avg("delivery_person_ratings"), 2).alias("avg_driver_rating"),
    countDistinct("delivery_person_id").alias("total_drivers")
)

gold_city_performance.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_city_performance/"
    )

# =====================================================
# GOLD 3 - TRAFFIC IMPACT
# =====================================================

gold_traffic_impact = df.groupBy(
    "road_traffic_density"
).agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time")
)

gold_traffic_impact.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_traffic_impact/"
    )

# =====================================================
# GOLD 4 - WEATHER IMPACT
# =====================================================

gold_weather_impact = df.groupBy(
    "weather_condition"
).agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time")
)

gold_weather_impact.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_weather_impact/"
    )

# =====================================================
# GOLD 5 - VEHICLE PERFORMANCE
# =====================================================

gold_vehicle_performance = df.groupBy(
    "type_of_vehicle"
).agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time")
)

gold_vehicle_performance.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_vehicle_performance/"
    )

# =====================================================
# GOLD 6 - DRIVER PERFORMANCE
# =====================================================

gold_driver_performance = df.groupBy(
    "delivery_person_id"
).agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time"),
    round(avg("delivery_person_ratings"), 2).alias("avg_driver_rating")
)

gold_driver_performance.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_driver_performance/"
    )

# =====================================================
# GOLD 7 - DAILY DELIVERY TRENDS
# =====================================================

daily_df = df.withColumn(
    "order_date",
    to_date("order_timestamp")
)

gold_daily_delivery_trends = daily_df.groupBy(
    "order_date"
).agg(
    count("*").alias("total_orders"),
    round(avg("time_taken_min"), 2).alias("avg_delivery_time"),
    round(avg("delivery_person_ratings"), 2).alias("avg_driver_rating")
)

gold_daily_delivery_trends.write \
    .mode("overwrite") \
    .parquet(
        "s3://food-delivery-logistics-data-project/gold/gold_daily_delivery_trends/"
    )

# =====================================================
# VALIDATION
# =====================================================

print("========== GOLD TABLE PREVIEW ==========")

gold_delivery_kpis.show()

gold_city_performance.show()

gold_traffic_impact.show()

gold_weather_impact.show()

gold_vehicle_performance.show()

gold_driver_performance.show(10, False)

gold_daily_delivery_trends.show(10, False)

# =====================================================
# SUCCESS MESSAGE
# =====================================================

print("All Gold tables created successfully")