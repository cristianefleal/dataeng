import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1699660822053 = glueContext.create_dynamic_frame.from_catalog(
    database="kinesislab",
    table_name="tb_despesas_detalhadas_raw",
    transformation_ctx="AWSGlueDataCatalog_node1699660822053",
)
# Script generated for node Amazon S3
AmazonS3_node1699661739320 = glueContext.write_dynamic_frame.from_options(
    frame=AWSGlueDataCatalog_node1699660822053,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://projetoimpacta-grupo0001-destination-silver/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1699661739320",
)

job.commit()
