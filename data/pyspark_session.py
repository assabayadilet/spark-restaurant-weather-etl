from pyspark.sql import SparkSession
#Adilet
# Create a SparkSession
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Print Spark session details
print("Spark Application Name:", spark.sparkContext.appName)
print("Spark Master URL:", spark.sparkContext.master)
print("Spark Version:", spark.version)
print("Spark Application ID:", spark.sparkContext.applicationId)
print("Spark Web UI URL:", spark.sparkContext.uiWebUrl)
print("Spark User:", spark.sparkContext.sparkUser())
print("Spark Configurations:")
for key, value in spark.sparkContext.getConf().getAll():
    print(f"  {key}: {value}")
