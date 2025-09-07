from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

if __name__ == "__main__":
    print("This is the main program.")

    def process_log_file(input_file, output_file):
        spark = SparkSession.builder.appName("LogFileProcessor").getOrCreate()
        
        # Read the webserver log file from a storage location
        logs_df = spark.read.text(input_file)        
        # Perform aggregation (example: count occurrences of each log level)
        # Extract IP address using regex and count occurrences
        ip_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        logs_df = logs_df.withColumn("ip_address", regexp_extract("value", ip_pattern, 1))
        aggregated_df = logs_df.groupBy("ip_address").count()
        
        # Write the results to a new file``
        aggregated_df.write.csv(output_file, header=True)

        
        spark.stop()

    # Example usage
    process_log_file("/Users/harishb/Downloads/archive", "/Users/harishb/Downloads/archive/aggregated_results.csv")

    