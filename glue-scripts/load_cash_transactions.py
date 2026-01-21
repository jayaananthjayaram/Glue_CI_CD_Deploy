import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# -----------------------------------
# Glue bootstrap
# -----------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------
# Read CSV from S3
# -----------------------------------
df = spark.read \
    .option("header", "true") \
    .csv("s3://samplefilesglue/empfile/emp.csv")

print("SOURCE DATA")
df.show()
df.printSchema()

# -----------------------------------
# Register temp view
# -----------------------------------
df.createOrReplaceTempView("emp_table")

# -----------------------------------
# Insert into Iceberg table
# -----------------------------------
spark.sql("""
INSERT INTO glue_catalog.finance.cash_transactions
SELECT
    TransactionId,
    PortfolioId,
    CashBookCode,
    Narration,
    TransactionType,
    to_date(TradeDate, 'dd/MM/yyyy')      AS TradeDate,
    to_date(SettlementDate, 'dd/MM/yyyy') AS SettlementDate,
    cast(Amount as double)               AS Amount
FROM emp_table
""")

print("Iceberg insert completed")

job.commit()