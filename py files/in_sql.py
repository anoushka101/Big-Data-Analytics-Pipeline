import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark SQL Queries") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
    .getOrCreate()

# Path to the input file (local)
input_file = "s3a://anoushka-bda/output.csv"

# Define S3 bucket path for output
s3_bucket = "s3a://anoushka-bda/sqlquery"

# Read CSV file into a DataFrame
df = spark.read.csv(input_file, header=True, inferSchema=True)

# Remove commas from 'actual_price' and 'selling_price' columns and cast to numeric
df = df.withColumn("actual_price", regexp_replace(col("actual_price"), ",", "").cast("float")) \
       .withColumn("selling_price", col("selling_price").cast("float"))

# Create a temporary SQL view
df.createOrReplaceTempView("products")

# Define SQL queries
queries = {
    "top_discounted_products": """
        SELECT _id, title, actual_price, selling_price, discount
        FROM products
        ORDER BY (actual_price - selling_price) DESC
        LIMIT 10
    """,
    "avg_rating_per_brand": """
        SELECT brand, AVG(average_rating) AS avg_rating
        FROM products
        WHERE average_rating IS NOT NULL
        GROUP BY brand
        ORDER BY avg_rating DESC
    """,
    "product_count_per_category": """
        SELECT category, COUNT(*) AS product_count
        FROM products
        GROUP BY category
        ORDER BY product_count DESC
    """,
    "most_popular_seller": """
        SELECT seller, COUNT(*) AS product_count
        FROM products
        GROUP BY seller
        ORDER BY product_count DESC
        LIMIT 1
    """,
    "out_of_stock_products": """
        SELECT title, brand, category
        FROM products
        WHERE out_of_stock = 'True'
    """
}

# Execute queries and save results to S3
for query_name, query in queries.items():
    print(f"Executing query: {query_name}")
    query_result = spark.sql(query)
    output_path = os.path.join(s3_bucket, f"{query_name}_output")
    query_result.write.csv(output_path, header=True)

print("Queries executed and results saved to S3.")
spark.exit()
