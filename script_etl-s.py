import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

def tratarColunas(rec):
    
    split_data = rec['numero_empenho'].split('NE')
    rec['ano'] = split_data[0]
    return rec

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1699660822053 = glueContext.create_dynamic_frame.from_catalog(
    database="kinesislab",
    table_name="tb_despesas_detalhadas_silver",
    transformation_ctx="AWSGlueDataCatalog_node1699660822053",
)

    
transformacao1 = Map.apply(
        frame=AWSGlueDataCatalog_node1699660822053, 
        f=tratarColunas, 
        transformation_ctx="transformacao1"
    )

# Script generated for node Drop Fields
DropFields_node1699661522498 = DropFields.apply(
    frame=transformacao1,
    paths=[
        "fonte_recurso",
        "obs",
        "cd_nm_modalidade",
        "cd_nm_grupo",
        "codigo_ug",
        "credor",
        "vlrtotalpago",
        "cd_nm_categoria",
        "ficha",
        "cd_nm_subacao",
        "numero_empenho",
        "cd_nm_elemento",
        "vlrliquidado",
    ],
    transformation_ctx="DropFields_node1699661522498",
)

# Script generated for node Aggregate
Aggregate_node1699661635065 = sparkAggregate(
    glueContext,
    parentFrame=DropFields_node1699661522498,
    groups=["ano", "unidade_gestora", "cd_nm_funcao", "ds_tp_desp"],
    aggs=[["vlrempenhado", "sum"], ["ano", "count"]],
    transformation_ctx="Aggregate_node1699661635065",
)

# Script generated for node Change Schema
ChangeSchema_node1699661689696 = ApplyMapping.apply(
    frame=Aggregate_node1699661635065,
    mappings=[
        ("ano", "string", "ano", "string"),
        ("unidade_gestora", "string", "unidade_gestora", "string"),
        ("cd_nm_funcao", "string", "cd_nm_funcao", "string"),
        ("ds_tp_desp", "string", "ds_tp_desp", "string"),
        ("`sum(vlrempenhado)`", "double", "valor_empenhado_total", "double"),
        ("`count(ano)`", "long", "quantidade_empenhos", "long"),
    ],
    transformation_ctx="ChangeSchema_node1699661689696",
)

# Script generated for node Amazon S3
AmazonS3_node1699661739320 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1699661689696,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://projetoimpacta-grupo0001-destination-gold/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1699661739320",
)

job.commit()
