import pyspark
import shutil
import os

def tokenize(text):
    return text.split()

def list_to_string(lst):
        return '_'.join(lst)

def generate_ngrams(input_rdd, n):
    RDD = input_rdd.map(tokenize)\
                   .flatMap(lambda line: [(list_to_string(line[i:i+n]), 1) for i in range(len(line)-n+1)])\
                   .reduceByKey(lambda x, y: x + y)\
                   .sortBy(lambda x: x[1], ascending=False)
    return RDD


# Set variables
worker_count = 6
file_size = 540
partition_size = 30
partition_count = int(file_size / partition_size)

# Set paths
home = "/home/ahmad/Programming/Python/Code/Spark-wordcount/"
source_folder = home + "txt/"
destination_folder = home
log = home + "log/"
input_dir = home + "input.txt"
output_name = "output3.txt"

# Manage and configure the SparkContext
conf = pyspark.SparkConf()\
  .setAppName("word_counter")\
  .set("spark.eventLog.enabled", "true") \
  .set("spark.eventLog.dir", log) \
  .setMaster("local[" + str(worker_count) + "]")

spark = pyspark.sql.SparkSession.builder\
  .config("spark.executor.memory", "10g") \
  .config("spark.driver.memory", "10g") \
  .config(conf=conf).getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile(input_dir)
rdd = rdd.repartition(partition_count)

# Doing the task
counts = generate_ngrams(rdd, 3)

# Ensure the output directory is empty before saving
output_dir = home + "txt/"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

counts.coalesce(1).saveAsTextFile(output_dir)

# Copy the part-00000 file outside of the folder
files = os.listdir(source_folder)
for file in files:
    if file == "part-00000":
        source_file = os.path.join(source_folder, file)
        destination_file = os.path.join(destination_folder, file)
        shutil.copyfile(source_file, destination_file)
        break
else:
    print("File 'part-00000' not found in the source folder.")

# Delete the source folder
if os.path.exists(source_folder):
    shutil.rmtree(source_folder)
else:
    print(f"The folder '{source_folder}' does not exist.")

# Rename the filename
os.rename(os.path.join(destination_folder, "part-00000"), os.path.join(destination_folder, output_name))
