from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when, split, sum as spark_sum, avg, regexp_replace
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
import logging

logger = logging.getLogger(__name__)


def createSparkSession():
    try:
        # Create SparkSession - ADD THIS
        spark = SparkSession.builder \
            .appName("DataProcessing") \
            .getOrCreate()
        logger.info('SparkSession created successfully.')
        return spark
    except Exception as e:
        logger.error(f'Error creating SparkSession: {e}')
        raise e


def read_data(spark, file_path):
    try:
    # Read data from CSV file into DataFrame
        data = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)
        logger.info(f"Succesfully read {data.count()} rows from CSV file.")
        return data
    except Exception as e:
        logger.error(f'Error reading data from CSV file: {e}')
        raise e


def main():
    logging.basicConfig(level=logging.INFO)

    spark = createSparkSession()
    data = read_data(spark, "/home/iceberg/notebooks/data/data/customer.csv")
    # Display the first 10 rows
    data.show(10)

    # Deduplicate rows based on Customer_ID
    data_unique = data.dropDuplicates()
    logger.info(f"Rows after deduplication: {data_unique.count()}")

    data_cleaned = data_unique.select(
                        col("c_custkey").alias("Customer_ID"),
                        col("c_name").alias("Customer_Name"),
                        col("c_address").alias("Customer_Address"),
                        col("c_nationkey").alias("Customer_NationKey"),
                        col("c_phone").alias("Customer_Phone"),
                        coalesce(col("c_acctbal"), lit(0.00)).alias("Customer_AccountBalance"),
                        coalesce(col("c_mktsegment"), lit("OTHER")).alias("Customer_MarketSegment"),
                        col("c_comment").alias("Customer_Comment")
                       ).withColumn("Customer_AccountTier", 
                                      when(col("Customer_AccountBalance") >= 5000, "Platinum")
                                      .when(col("Customer_AccountBalance") >= 1000, "Gold")
                                      .when(col("Customer_AccountBalance") >= 500, "Silver")
                                      .otherwise("Bronze")
                                    )
    
    try:
        data_cleaned.where((col("Customer_AccountBalance") == 0) | (col("Customer_MarketSegment") == "OTHER")).show()
        logger.info("Data cleaning and transformation completed successfully.")
    except Exception as e:
        logger.error(f'Error during data cleaning and transformation: {e}')
        raise e
    
    account_tier_counts = data_cleaned.groupBy("Customer_AccountTier").count()
    account_tier_counts.show()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()