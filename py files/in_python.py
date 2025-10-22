from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc, max, regexp_replace,split, regexp_extract

# Start Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Load data from S3
input_path = "s3a://anoushka-bda/output.csv"
output_path = "s3a://anoushka-bda/aggregartion_output"

# Read input data
df = spark.read.csv(input_path, header=True, inferSchema=True)

df = df.drop('Unnamed: 0')
df=df.dropna()

# Transformations
# Step 1: Extract time from crawled_at
df = df.withColumn("Time", split(col("crawled_at"), ", ")[1])

# Step 2: Remove ',' from actual_price and selling_price and calculate discount_price
df = df.withColumn("actual_price", regexp_replace(col("actual_price"), ",", "").cast("float")) \
                           .withColumn("selling_price", regexp_replace(col("selling_price"), ",", "").cast("float")) \
                           .withColumn("discount_price", (col("actual_price") - col("selling_price")))

# Step 3: Switch names of description and discount columns
df = df.withColumnRenamed("description", "discount1") 
df_new = df.withColumnRenamed("discount", "description")                   
df = df_new.withColumnRenamed("discount1", 'discount')
df_final = df.withColumn("discount", regexp_extract(col("discount"), r"(\d+)", 1).cast("float"))
df_final.show(5)

# Aggregations
revenue_by_brand = df_final.groupBy("brand").agg(sum("selling_price").alias("Total_Revenue"))
avg_discount_by_category = df_final.groupBy("category").agg(avg("discount").alias("Avg_Discount_Percentage"))
top_sellers = df_final.groupBy("seller").agg(sum("selling_price").alias("Total_Sales")).orderBy(desc("Total_Sales")).limit(5)
product_count = df_final.groupBy("sub_category").agg(count("*").alias("Product_Count"))
max_discount = df_final.agg(max("discount").alias("Max_Discount_Percentage"))

# Save outputs to S3 as single CSV files
revenue_by_brand.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "revenue_by_brand")
avg_discount_by_category.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "avg_discount_by_category")
top_sellers.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "top_sellers")
product_count.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "product_count")
max_discount.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path + "max_discount")

print("Aggregated files saved to S3.")
spark.stop()
