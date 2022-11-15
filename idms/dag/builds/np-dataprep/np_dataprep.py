# TODO remove unused imports
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from da_consumer_odbx import transform
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import MapType, StringType
import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
from itertools import chain
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
# import boto3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, LongType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job.commit()

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME"
        # ,
        # "in_path",
        # "out_path",
        # "lookup_path",
        # "l2_path",
        # "join_path",
        # "temp_path",
    ],
)

# in_path = args["in_path"]
# out_path = args["out_path"]
# lookup_path = args["lookup_path"]
# l2_path = args["l2_path"]
# join_path = args["join_path"]
# temp_path = args["temp_path"]

"""
***************  Schemas   ***************  
"""
# schema of donorbase_transaction
txn_schema = StructType([
    StructField("ListID", LongType()),
    StructField("ID", IntegerType()),
    StructField("Individual_ID", StringType()),
    StructField("HH_ID", StringType()),
    StructField("AccountNo", StringType()),
    StructField("ListCategory01", StringType()),
    StructField("List_TotalNumberDonations", IntegerType()),
    StructField("ListCategory02", StringType()),
    StructField("ListCategory03", StringType()),
    StructField("ListCategory04", StringType()),
    StructField("ListCategory05", StringType()),
    StructField("List_TotalDollarDonations", IntegerType()),
    StructField("List_LastDateDonation", StringType()),
    StructField("List_FirstDateDonation", StringType()),
    StructField("List_LastPaymentMethod", StringType()),
    StructField("List_LastChannel", StringType()),
    StructField("List_LastDollarDonation", FloatType()),
    StructField("List_HighestDollarDonation", FloatType()),
    StructField("List_LowestDollarDonation", FloatType()),
    StructField("List_WeeksSinceLastDonation", IntegerType()),
    StructField("List_VolunteerInd", StringType()),
    StructField("Detail_DonationDollar", FloatType()),
    StructField("Detail_DonationDate", StringType()),
    StructField("Detail_PaymentMethod", IntegerType()),
    StructField("Detail_DonationChannel", StringType())
])
# schema of mail file
mail_schema = StructType([
    StructField("ID", LongType()),
    StructField("ListID", LongType()),
    StructField("AccountNo", StringType()),
    StructField("Individual_ID", StringType()),
    StructField("company_id", StringType()),
    StructField("MailDate", DateType()),
    StructField("SourceCode1", StringType()),
    StructField("SourceCode2", StringType()),
    StructField("SourceCode3", StringType()),
    StructField("SourceCode4", StringType()),
    StructField("SourceCode5", StringType()),
    StructField("MailFile", StringType())
])
# response file schema (response file contains donation details)
response_schema = StructType([
    StructField("ID", LongType()),
    StructField("ListID", LongType()),
    StructField("AccountNo", StringType()),
    StructField("Individual_ID", StringType()),
    StructField("company_id", StringType()),
    StructField("Detail_DonationDollar", FloatType()),
    StructField("Detail_DonationDate", StringType()),
    StructField("Detail_PaymentMethod", StringType()),
    StructField("Detail_DonationChannel", StringType()),
    StructField("SourceCode1", StringType()),
    StructField("SourceCode2", StringType()),
    StructField("SourceCode3", StringType()),
    StructField("SourceCode4", StringType()),
    StructField("SourceCode5", StringType())
])

"""
***************  S3 file paths and parameters  ***************  
"""

client = "ALCA"  # client name
# jtk = "DON-346/lin"  # kanban board model number
listid = '19348'  # unique ID in Donorbase validation report
maildates = ["2016-10-25", "2017-10-24"]  # mail dates for each file.
days = 90  # Number of days for inferred match window
mail_files = ['21407.txt',  # 330 ALICE LLOYD COLLEGE - REG MAILER 1
              '21408.txt']  # 330 ALICE LLOYD COLLEGE - REG MAILER 2
response_files = ["21409.txt", "21094.txt"]


bucket_name = "axle-donorbase-silver-sources"
donorbase_s3_folder = f"s3://{bucket_name}/exports/transactions/19348/"
mail_responce_files_s3_folder = f"s3://{bucket_name}/mail_responder/"
response_file_s3_path = f"{mail_responce_files_s3_folder}21409.txt"
response_file_s3_path1 = f"{mail_responce_files_s3_folder}21094.txt"
output_s3_folder = f"s3://{bucket_name}/donorbase-clients/{client}/{jtk}/response/raw/"
output_file_name = "response.csv"

# for testing locally
local_folder = "/Users/prarthana/Downloads/"
donorbase_local_folder = f"{local_folder}donorbase_transaction.csv"
mail_responce_files_s3_folder = local_folder

donorbase_s3_folder = donorbase_local_folder
mail_responce_files_s3_folder = local_folder
response_file_s3_path = f"{local_folder}/21409.txt"
response_file_s3_path1 = f"{local_folder}/21094.txt"
mail_files = ['21094.txt']
mail_schema = StructType([
    StructField("ID", LongType()),
    StructField("ListID", LongType()),
    StructField("AccountNo", StringType()),
    StructField("Individual_ID", StringType()),
    StructField("company_id", StringType()),
    StructField("MailDate", StringType()),
    StructField("SourceCode1", StringType()),
    StructField("SourceCode2", StringType()),
    StructField("SourceCode3", StringType()),
    StructField("SourceCode4", StringType()),
    StructField("SourceCode5", StringType()),
    StructField("MailFile", StringType())
])
"""
*************** Functions  ***************  
"""

def read_file(s3_path, schema):
    return spark.read.schema(schema).option("sep", "|").csv(s3_path)

def read_mail_df(mail_files_list):
    mail_df = spark.createDataFrame([], mail_schema)
    mail_index = 1  # literal used to distingush the mail files
    for txt_file in mail_files_list:
        mail = (read_file(f"{mail_responce_files_s3_folder}/{txt_file}", mail_schema)
                .withColumn('MailFile', F.lit(mail_index))
                #          .withColumn("MailDate",F.lit("2016-10-25"))
                )
        print("Distinct date for mail file " + txt_file + " is :", mail.select("MailDate").distinct().collect())
        print("Mail File : ", mail_index)
        mail_df = mail_df.union(mail)
        mail_index = mail_index + 1
    return mail_df.select("ListID", "Individual_ID", F.col("company_id").alias("HH_ID"), "MailDate",
                             F.col("SourceCode1").alias("SourceCode"), "MailFile")

# function raises value error when the maildates are not in the donation date window of the responder file.
def check_time_window(df, maildates, days):
    min_date = df.agg(F.min(F.col("Detail_DonationDate"))).collect()[0][0]
    min_date = datetime.combine(min_date, datetime.min.time())
    print("Minimum date on transactions file is", min_date.strftime("%Y-%m-%d"))
    max_date = df.agg(F.max(F.col("Detail_DonationDate"))).collect()[0][0]
    max_date = datetime.combine(max_date, datetime.min.time())
    print("Maximum date on transactions file is", max_date.strftime("%Y-%m-%d"))
    for m in maildates:
        if (
                (datetime.strptime(m, "%Y-%m-%d") + timedelta(days=days) <= max_date) &
                (datetime.strptime(m, "%Y-%m-%d") >= min_date)
        ):
            continue
        raise ValueError((f"{m} mail date is not within transactions time window"))

def write_file(acq_resp_file, output_s3_folder, output_file_name):
    # TODO used for testing, change this
    acq_resp_file.write.mode("overwrite")\
        .csv("s3://idms-2722-playground/prarthana/dag_output/")
        # .csv(output_s3_folder + output_file_name)
    # acq_resp_file_pd = acq_resp_file.toPandas().to_csv(output_file_name, index=False)
    # s3 = boto3.client("s3")
    # s3.upload_file(
    #     Bucket=bucket_name,
    #     Key=output_s3_folder + output_file_name,
    #     Filename=output_file_name)
    # print(f"Successfully writen the responce file to path: {output_s3_folder}{output_file_name}")
    return


"""
*************** Reading data  ***************  
"""

donorbase_transaction = read_file(donorbase_s3_folder, txn_schema).filter(F.col("ID") == listid)

# reading of mail files from S3 bucket and join the files to create a single dataframe
mail_df = read_mail_df(mail_files)

# reading transaction detail from Donorbase transaction table.
df_resp = (donorbase_transaction
           .select("Individual_ID", "hh_id", "Detail_DonationDollar", "Detail_DonationDate")
           .withColumn("Detail_DonationDate", F.to_date("Detail_DonationDate", "yyyyMMdd"))
           .withColumn("SourceCode", F.monotonically_increasing_id()))

# reading the responder file
responder = (read_file(response_file_s3_path1, response_schema)
             .withColumn("Detail_DonationDate", F.to_date("Detail_DonationDate", "yyyyMMdd"))
             .drop('Individual_ID', "company_id")
             .select("AccountNo", "Detail_DonationDollar", "Detail_DonationDate",
                     F.col("SourceCode1").alias("SourceCode"))
             )

"""
*************** EDA  ***************  
"""
donorbase_transaction.show()
mail_df.show()
mail_df.groupBy("MailDate", "MailFile").count().show(5)
# returns the donation distribution over month-year
# this piece of code can used used to identify which month data can used in modeling and the inferred window size.
df_resp.join(mail_df, 'Individual_ID').withColumn('year', F.col('Detail_DonationDate').substr(1, 7)).groupBy(
    'year').count().show()
# returns the donation distribution over dates
# this piece of code can be used to identify the exact dates
df_resp.join(mail_df, 'Individual_ID').withColumn('year', F.col('Detail_DonationDate')).groupBy('year').count().show()

# check if source code is null
res = read_file(response_file_s3_path, response_schema).withColumn("Detail_DonationDate", F.to_date("Detail_DonationDate", "yyyyMMdd"))
res.show(5)

# reading the transaction details from donorbase transaction table using uniqur IDs
df_trans = (donorbase_transaction
            .select("AccountNo", "Individual_ID", "hh_id").drop_duplicates(["AccountNo"])
            #          .withColumn("SourceCode",F.monotonically_increasing_id()) #this piece of code is used when source code is null in response file
            )
df_trans.show(5)
# joining the response file with transaction file on "Account number"
df_resp = responder.join(df_trans, 'AccountNo').drop("AccountNo")
# response file match count with transaction table
responder.count(), df_resp.count(), responder.count() - df_resp.count()
# reading transaction detail from Donorbase transaction table.
df_resp = (donorbase_transaction
           .select("Individual_ID", "hh_id", "Detail_DonationDollar", "Detail_DonationDate")
           .withColumn("Detail_DonationDate", F.to_date("Detail_DonationDate", "yyyyMMdd"))
           .withColumn("SourceCode", F.monotonically_increasing_id()))
# returns donation date window of the respnders.
df_resp.select(F.min("Detail_DonationDate"),
               F.max("Detail_DonationDate")).show()

check_time_window(df_resp, maildates, days)
# suppress df contains records that are with in 2 years.

for index, value in enumerate(maildates, 1):
    value = (
        df_resp.filter(
            F.col("Detail_DonationDate").between(
                datetime.strptime(value, "%Y-%m-%d") - timedelta(days=730),
                datetime.strptime(value, "%Y-%m-%d"),
            )
        )
        .select("individual_id")
        .drop_duplicates(["individual_id"])
        .withColumn("mailfile", F.lit(index))
        .withColumn("suppress", F.lit(1))
    )

    if index == 1:
        suppress = value
    else:
        suppress = suppress.union(value)

suppress.groupBy("mailfile").count().show()  # count of responder for each file after supression
# df_mail contains records that are older than 2 years.
print("total records before suppression: {}".format(mail_df.count()))

df_mail = mail_df.join(suppress, on=["individual_id", "mailfile"], how="left_anti").drop(
    "suppress"
)

print("total records after suppression: {}".format(df_mail.count()))
# renaming the columns
df_resp = df_resp.withColumnRenamed("Detail_DonationDollar", "gift_amount").withColumnRenamed("Detail_DonationDate",
                                                                                              "giftdate")
resp_ind_sc = (
    df_resp.groupby("individual_id", "SourceCode")
    .agg(
        F.sum(F.col("gift_amount")).alias("tot_gift_amount"),
        F.count(F.col("gift_amount")).alias("n_of_dons"),
        F.mean(F.col("gift_amount")).alias("avg_gift"),
        F.min(F.col("giftdate")).alias("giftdate"),
    )
    .filter(F.col("n_of_dons") < 500)
    .withColumn("responder", F.lit(1))
)
resp_ind_sc.limit(10).show()
#  removing house hold IDs which has more than 500 donation records.
resp_hh_sc = (df_resp.groupby("HH_ID", "SourceCode")
              .agg(
    F.sum(F.col("gift_amount")).alias("tot_gift_amount"),
    F.count(F.col("gift_amount")).alias("n_of_dons"),
    F.mean(F.col("gift_amount")).alias("avg_gift"),
    F.min(F.col("giftdate")).alias("giftdate"),
)
              .filter(F.col("n_of_dons") < 500)
              .withColumn("responder", F.lit(1))
              )
resp_hh_sc.groupby("n_of_dons").count().show()
# joining mail & response files on sc & [ind_id, hh_id]

mail_ind_sc = df_mail.drop_duplicates(["individual_id", "sourcecode"])
mail_hh_sc = df_mail.drop_duplicates(["hh_id", "sourcecode"])

mail_resp_ind_sc = mail_ind_sc.join(
    resp_ind_sc, on=["individual_id", "sourcecode"], how="left"
).fillna(0)

mail_resp_hh_sc = mail_hh_sc.join(
    resp_hh_sc, on=["hh_id", "sourcecode"], how="left"
).fillna(0)

print(
    "Ind ID & Source Code RR: {}".format(
        round(
            mail_resp_ind_sc.filter(F.col("responder") == 1).count()
            / mail_resp_ind_sc.count()
            * 100,
            2,
        )
    )
)
print("# Responders: {}".format(mail_resp_ind_sc.filter(F.col("responder") == 1).count()))
print("# Mailed: {}".format(mail_resp_ind_sc.count()))

print(
    "HH ID & Source Code RR: {}".format(
        round(
            mail_resp_hh_sc.filter(F.col("responder") == 1).count()
            / mail_resp_hh_sc.count()
            * 100,
            2,
        )
    )
)
print("# Responders: {}".format(mail_resp_hh_sc.filter(F.col("responder") == 1).count()))
print("# Mailed: {}".format(mail_resp_hh_sc.count()))
# Rolling window match to identify the responder in each mail file using response file
for index, value in enumerate(maildates, 1):
    # grabs the records with in the inferred time window from df_resp
    resp = (
        df_resp
        .filter(
            F.col("giftdate").between(
                datetime.strptime(value, "%Y-%m-%d"),
                datetime.strptime(value, "%Y-%m-%d") + timedelta(days=days),
            )
        )
    ).drop("hh_id", "sourcecode")

    # df agg on gift_amt and grouped by Ind_ID
    resp_agg = (
        resp
        .groupby("individual_id")
        .agg(
            F.sum(F.col("gift_amount")).alias("tot_gift_amount"),
            F.count(F.col("gift_amount")).alias("n_of_dons"),
            F.mean(F.col("gift_amount")).alias("avg_gift"),
            F.min(F.col("giftdate")).alias("giftdate"),
        )
        .withColumn("responder", F.lit(1))
    )

    # left-join df_mail & agg df on ind_id
    mail_resp = (
        df_mail
        .filter(F.col("mailfile") == index)
        .dropDuplicates(["individual_id"])
        .join(
            resp_agg, how="left", on="individual_id"
        )
    ).fillna(0)
    # appending the created df for each mail-date.
    if index == 1:
        mail_resp_ind_window = mail_resp
    else:
        mail_resp_ind_window = mail_resp_ind_window.unionByName(mail_resp)
# Rolling window match to identify the responder in each mail file using transaction data
for index, value in enumerate(maildates, 1):
    resp = (
        donorbase_transaction
        .withColumn("Detail_DonationDate", F.to_date(F.col("Detail_DonationDate"), "yyyyMMdd"))
        .filter(
            F.col("Detail_DonationDate").between(
                datetime.strptime(value, "%Y-%m-%d"),
                datetime.strptime(value, "%Y-%m-%d") + timedelta(days=days),
            )
        )
    ).drop("hh_id")

    resp_agg = (
        resp
        .groupby("individual_id")
        .agg(
            F.sum(F.col("Detail_DonationDollar")).alias("tot_gift_amount"),
            F.count(F.col("Detail_DonationDollar")).alias("n_of_dons"),
            F.mean(F.col("Detail_DonationDollar")).alias("avg_gift"),
            F.min(F.col("Detail_DonationDate")).alias("giftdate"),
        )
        .withColumn("responder", F.lit(1))
    )

    mail_resp = (
        df_mail
        .filter(F.col("mailfile") == index)
        .dropDuplicates(["individual_id"])
        .join(
            resp_agg, how="left", on="individual_id"
        )
    ).fillna(0)

    if index == 1:
        mail_txn_ind_window = mail_resp
    else:
        mail_txn_ind_window = mail_txn_ind_window.unionByName(mail_resp)
# this is over response individual window
# using pivot to find count of 0s and 1s
df_resp_new = mail_resp_ind_window.groupBy("mailfile").pivot("responder").count()

# creating a dataframe which conatins each mail file summary.(os ,1s ,RR ,total_mails)
df_resp_new = df_resp_new.withColumn(
    "total_mailed", F.sum(F.col("0") + F.col("1")).over(Window.partitionBy("mailfile"))
).withColumn("RR", F.round(F.col("1") / F.col("total_mailed") * 100, 3))
a = df_resp_new.agg(F.sum("total_mailed")).collect()[0][0]
b = df_resp_new.agg(F.sum("1")).collect()[0][0]

print("total mail quantity: {}".format(a))
print("total response quantity: {}".format(b))
print("overall RR: {}".format(round(b / a * 100, 2)))
df_resp_new.show()
# this is over transaction individual window
# using pivot to find count of 0s and 1s
df = mail_txn_ind_window.groupBy("mailfile").pivot("responder").count()

# creating a dataframe which conatins each mail file summary.(os ,1s ,RR ,total_mails)
df = df.withColumn(
    "total_mailed", F.sum(F.col("0") + F.col("1")).over(Window.partitionBy("mailfile"))
).withColumn("RR", F.round(F.col("1") / F.col("total_mailed") * 100, 3))
a = df.agg(F.sum("total_mailed")).collect()[0][0]
b = df.agg(F.sum("1")).collect()[0][0]
rr = round(b / a * 100, 2)
print("total mail quantity: {}".format(a))
print("total response quantity: {}".format(b))
print("overall RR: {}".format())
df.show()
# selecting required columns to store in S3
acq_resp_file = mail_resp_ind_window.select("individual_id", "SourceCode", "mailfile", "responder", "avg_gift")
acq_resp_file.show()
# finding average gift amount for each file
# the null in mail file represents average gift amount of all the files.
acq_resp_file.filter(F.col("responder") == 1).rollup("mailfile").agg(
    F.round(F.avg("avg_gift"), 2).alias("avg_gift"), ).show()

write_file(acq_resp_file, output_s3_folder, output_file_name)
