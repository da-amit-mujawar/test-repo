import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from da_consumer_odbx import transform
from pyspark.sql.functions import udf, struct, substring, concat_ws, col
from pyspark.sql.types import MapType, StringType

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
        "in_path",
        "out_path",
        "lookup_path",
        "l2_path",
        "join_path",
        "temp_path",
        "home_appr_path",
        "odb_path",
        "input_file_type",
    ],
)

in_path = args["in_path"]
out_path = args["out_path"]
lookup_path = args["lookup_path"]
l2_path = args["l2_path"]
join_path = args["join_path"]
temp_path = args["temp_path"]
home_appr_path = args["home_appr_path"]
odb_path = args["odb_path"]
input_file_type = args["input_file_type"]

state_name = in_path.split("/")[-1][:4]
output_path = "{}{}{}".format(out_path, state_name, "/")

odbx_udf = udf(transform.transform, MapType(StringType(), StringType()))

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

happrdf = (
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
    odbx_df = spark.read.format("avro").load(in_path)
elif input_file_type == "json":
    odbx_df = spark.read.format("json").load(in_path)

join_df1 = odbx_df.join(
    nested_ldf, odbx_df["familyid"] == nested_ldf["lookup.familyid"], "inner"
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
        happrdf,
        (fips_df["fips"] == happrdf["a_fips"])
        & (fips_df["l4_mvy"] == happrdf["saleyear"]),
        "left",
    )
    .drop(*("l4_mvy", "saleyear", "a_fips"))
    .withColumnRenamed("apprate", "home_appreciation_rate_by_market_year")
)

sales_year_appr_df = (
    market_year_appr_df.join(
        happrdf,
        (market_year_appr_df["fips"] == happrdf["a_fips"])
        & (market_year_appr_df["l4_sd"] == happrdf["saleyear"]),
        "left",
    )
    .drop(*("l4_sd", "saleyear", "a_fips", "fips"))
    .withColumnRenamed("apprate", "home_appreciation_rate_by_sale_year")
)

sales_year_appr_df.write.mode("overwrite").json(join_path)

joined_df = spark.read.option("ignoreLeadingWhiteSpace", True).text(join_path)

transformed_odbx_df = joined_df.withColumn("odbx", odbx_udf(joined_df.value))

transformed_odbx_df.select("odbx").write.mode("overwrite").json(temp_path)

drop_df = (
    spark.read.json(temp_path)
    .select("odbx.*")
    .drop(
        *(
            "lookup",
            "l2",
            "odb",
            "home_appreciation_rate_by_sale_year",
            "home_appreciation_rate_by_market_year",
            "address",
            "individual",
            "lifestyle_models",
            "lifestyles",
            "property",
            "telephone",
        )
    )
)

select_odbx_df = odbx_df.select(
    "familyid",
    "address",
    "individual",
    "lifestyle_models",
    "lifestyles",
    "property",
    "telephone",
).withColumnRenamed("familyid", "a_familyid")

write_df = drop_df.join(
    select_odbx_df, drop_df["familyid"] == select_odbx_df["a_familyid"], "left"
).drop("a_familyid")

write_df.repartition(1).write.mode("overwrite").json(output_path, compression="gzip")
