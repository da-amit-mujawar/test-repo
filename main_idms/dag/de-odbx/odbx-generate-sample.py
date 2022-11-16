import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import struct, col, substring, concat_ws

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
        "JOB_NAME",
        "odbx_path",
        "odb_path",
        "out_path",
        "lookup_path",
        "l2_path",
        "home_appr_path",
        "input_file_type",
    ],
)

odbx_path = args["odbx_path"]
odb_path = args["odb_path"]
out_path = args["out_path"]
lookup_path = args["lookup_path"]
l2_path = args["l2_path"]
home_appr_path = args["home_appr_path"]
input_file_type = args["input_file_type"]

drop_collist = {
    "census_tract_2010",
    "census_bg_2010",
    "zip_code",
    "sesi",
    "census_state_2010",
    "coordinate_match_level",
    "census_county_2010",
}
l2_df = spark.read.parquet(l2_path)
nested_l2df = l2_df.select(struct(*l2_df.columns).alias("l2"))

happr_df = (
    spark.read.option("header", True)
    .csv(home_appr_path, sep=",",)
    .withColumnRenamed("fips", "a_fips")
)

lookup_df = (
    spark.read.option("header", True).csv(lookup_path, sep="|").drop(*drop_collist)
)

odb_df = spark.read.option("header", True).csv(odb_path, sep=",")

nested_ldf = lookup_df.select(struct(*lookup_df.columns).alias("lookup"))
nested_odb = odb_df.select(struct(*odb_df.columns).alias("odb"))

if input_file_type == "avro":
    odbx_df = spark.read.format("avro").load(odbx_path)
elif input_file_type == "json":
    odbx_df = spark.read.format("json").load(odbx_path)

join_df1 = odbx_df.join(
    nested_ldf, odbx_df["familyid"] == nested_ldf["lookup.familyid"], "left"
)

join_df2 = join_df1.join(
    nested_l2df, join_df1["familyid"] == nested_l2df["l2.company_id"], "left"
)

final_df = join_df2.join(
    nested_odb, join_df2["familyid"] == nested_odb["odb.familyid"], "left"
)

fips_df = (
    final_df.withColumn(
        "fips",
        ((col("census_state_2010") * 1000) + col("census_county_2010"))
        .cast("int")
        .cast("string"),
    )
    .withColumn("l4_mvy", substring("odb.market_value_year0", 1, 4))
    .withColumn("l4_sd", substring(concat_ws(",", col("property.sale_date")), 1, 4))
)

market_year_appr_df = (
    fips_df.join(
        happr_df,
        (fips_df["fips"] == happr_df["a_fips"])
        & (fips_df["l4_mvy"] == happr_df["saleyear"]),
        "left",
    )
    .drop(*("l4_mvy", "saleyear", "a_fips"))
    .withColumnRenamed("apprate", "home_appreciation_rate_by_market_year")
)

sales_year_appr_df = (
    market_year_appr_df.join(
        happr_df,
        (market_year_appr_df["fips"] == happr_df["a_fips"])
        & (market_year_appr_df["l4_sd"] == happr_df["saleyear"]),
        "left",
    )
    .drop(*("l4_sd", "saleyear", "a_fips", "fips"))
    .withColumnRenamed("apprate", "home_appreciation_rate_by_sale_year")
)

sales_year_appr_df.repartition(1).write.mode("overwrite").json(
    out_path, compression="gzip"
)
