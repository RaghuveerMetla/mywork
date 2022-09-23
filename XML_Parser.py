import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["bucket_name", "file_name", "output_path"])
bucket_name = args['bucket_name']
file_name = args['file_name']
output_path = args['output_path']
print(bucket_name)
print(file_name)
print(output_path)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
read_xml = spark.read.format("com.databricks.spark.xml").option("rowTag", "RunInfo").load("s3://rio-ops-data/runs/run_1/RunInfo.xml")
#read_xml.printSchema()
read_xml.show(50,False)
#read_xml.withColumn('ImageChannels_col', explode(col('ImageChannels')['Name'])).show(50, False)
read_xml.write.json("s3://rio-ops-data/output_directory/xml_to_json/")

job.commit()
