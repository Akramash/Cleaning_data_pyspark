import pandas as pd
from pyspark.sql import SparkSession, functions as F

# Initialize Spark session
# This section initializes a Spark session which is the entry point to using Spark functionality.
# The application name 'cleaning_orders_dataset_with_pyspark' helps in identifying the application.
spark = (
    SparkSession
    .builder
    .appName('cleaning_orders_dataset_with_pyspark')
    .getOrCreate()
)

# Read the parquet file
# This section reads the input Parquet file 'orders_data.parquet' into a Spark DataFrame.
orders_data = spark.read.parquet('orders_data.parquet')

# Remove orders placed between 12am and 5am (inclusive)
# This section filters out orders that were placed between midnight (12:00 AM) and 5:00 AM inclusive.
orders_data = orders_data.filter(
    (F.hour('order_date') >= 5) & (F.hour('order_date') < 24)
)

# Add the 'time_of_day' column before converting order_date to date
# This section adds a new column 'time_of_day' which categorizes the orders into:
# - 'morning' for orders placed between 5 AM and 12 PM
# - 'afternoon' for orders placed between 12 PM and 6 PM
# - 'evening' for orders placed between 6 PM and midnight
orders_data = orders_data.withColumn(
    'time_of_day',
    F.when((F.hour('order_date') >= 5) & (F.hour('order_date') < 12), 'morning')
     .when((F.hour('order_date') >= 12) & (F.hour('order_date') < 18), 'afternoon')
     .when((F.hour('order_date') >= 18) & (F.hour('order_date') < 24), 'evening')
)

# Convert order_date from timestamp to date
# This section converts the 'order_date' column from a timestamp to just a date, removing the time part.
orders_data = orders_data.withColumn('order_date', F.to_date('order_date'))

# Remove rows containing "TV" in the product column and ensure all values are lowercase
# This section filters out any rows where the product name contains 'TV' (case insensitive).
# It also ensures that all product names are in lowercase.
orders_data = orders_data.filter(~F.lower(orders_data.product).like('%tv%'))
orders_data = orders_data.withColumn('product', F.lower('product'))

# Ensure all values in category column are lowercase
# This section converts all values in the 'category' column to lowercase.
orders_data = orders_data.withColumn('category', F.lower('category'))

# Add the 'purchase_state' column
# This section extracts the state abbreviation from the 'purchase_address' column and adds it as a new column 'purchase_state'.
orders_data = orders_data.withColumn(
    'purchase_state',
    F.split(F.col('purchase_address'), ', ')[2].substr(1, 2)
)

# Save the cleaned data to a new parquet file with overwrite mode
# This section writes the cleaned DataFrame to a new Parquet file named 'orders_data_clean.parquet' in the 'output_directory'.
# The 'overwrite' mode ensures that if the file already exists, it will be replaced.
orders_data.write.mode('overwrite').parquet('output_directory/orders_data_clean.parquet')

# Read the cleaned parquet file
# This section reads the cleaned Parquet file back into a Spark DataFrame to verify the cleaning process.
cleaned_data = spark.read.parquet('output_directory/orders_data_clean.parquet')

# Convert the cleaned Spark DataFrame to a Pandas DataFrame
# This section collects the Spark DataFrame into a Pandas DataFrame for easier inspection and better visualization.
pandas_df = cleaned_data.toPandas()

# Display the Pandas DataFrame
# This section prints the first 20 rows of the cleaned data as a Pandas DataFrame.
# This is useful for visual inspection in environments such as Jupyter Notebooks.
print(pandas_df.head(20))