
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =====================================================
# INITIALIZE SPARK
# =====================================================

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# =====================================================
# EXPLICIT SCHEMA
# =====================================================

schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Delivery_person_ID", StringType(), True),
    StructField("Delivery_person_Age", IntegerType(), True),
    StructField("Delivery_person_Ratings", DoubleType(), True),
    StructField("Restaurant_latitude", DoubleType(), True),
    StructField("Restaurant_longitude", DoubleType(), True),
    StructField("Delivery_location_latitude", DoubleType(), True),
    StructField("Delivery_location_longitude", DoubleType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Time_Orderd", StringType(), True),
    StructField("Time_Order_picked", StringType(), True),
    StructField("Weatherconditions", StringType(), True),
    StructField("Road_traffic_density", StringType(), True),
    StructField("Vehicle_condition", IntegerType(), True),
    StructField("Type_of_order", StringType(), True),
    StructField("Type_of_vehicle", StringType(), True),
    StructField("multiple_deliveries", DoubleType(), True),
    StructField("Festival", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Time_taken(min)", StringType(), True)
])

# =====================================================
# READ BRONZE DATA
# =====================================================

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(
        "s3://food-delivery-logistics-data-project/bronze/historical/train/"
    )

# =====================================================
# REMOVE DUPLICATES
# =====================================================

df = df.dropDuplicates()

# =====================================================
# REMOVE RECORDS WITH CRITICAL MISSING VALUES
# =====================================================

df = df.na.drop(
    subset=[
        "Delivery_person_Age",
        "Delivery_person_Ratings",
        "multiple_deliveries"
    ]
)

# =====================================================
# TRIM SPACES FROM ALL STRING COLUMNS
# =====================================================

for column_name, datatype in df.dtypes:
    if datatype == "string":
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )

# =====================================================
# FILL MISSING VALUES
# =====================================================

df = df.fillna({
    "Festival": "No",
    "Road_traffic_density": "Unknown",
    "Weatherconditions": "Unknown"
})

df = df.filter(
    col("Delivery_person_Ratings") <= 5.0
)

# =====================================================
# CLEAN WEATHER COLUMN
# =====================================================

df = df.withColumn(
    "weather_condition",
    regexp_replace(
        col("Weatherconditions"),
        "conditions ",
        ""
    )
)

# =====================================================
# CLEAN DELIVERY TIME COLUMN
# =====================================================

df = df.withColumn(
    "time_taken_min",
    regexp_extract(
        col("Time_taken(min)"),
        r"(\d+)",
        1
    ).cast("int")
)

# =====================================================
# CREATE ORDER TIMESTAMP
# =====================================================

df = df.withColumn(
    "order_timestamp",
    to_timestamp(
        concat_ws(
            " ",
            col("Order_Date"),
            col("Time_Orderd")
        ),
        "dd-MM-yyyy HH:mm:ss"
    )
)

# =====================================================
# CREATE PICKUP TIMESTAMP
# =====================================================

df = df.withColumn(
    "pickup_timestamp",
    to_timestamp(
        concat_ws(
            " ",
            col("Order_Date"),
            col("Time_Order_picked")
        ),
        "dd-MM-yyyy HH:mm:ss"
    )
)

# =====================================================
# ADD PARTITION COLUMNS
# =====================================================

df = df.withColumn(
    "year",
    year(col("order_timestamp"))
)

df = df.withColumn(
    "month",
    month(col("order_timestamp"))
)

# =====================================================
# DROP RAW / DIRTY COLUMNS
# =====================================================

df = df.drop(
    "Order_Date",
    "Time_Orderd",
    "Time_Order_picked",
    "Weatherconditions",
    "Time_taken(min)"
)

# =====================================================
# VALIDATION
# =====================================================

print("Rows after cleaning:", df.count())

df.printSchema()

df.select(
    "order_timestamp",
    "pickup_timestamp",
    "weather_condition",
    "time_taken_min"
).show(10, False)

# =====================================================
# WRITE SILVER DATA AS PARQUET
# =====================================================

df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(
        "s3://food-delivery-logistics-data-project/silver/deliveries/"
    )

print("Silver layer written successfully")