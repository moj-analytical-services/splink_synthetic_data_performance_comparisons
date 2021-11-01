import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext


import pyspark.sql.functions as f


from splink.truth import labels_with_splink_scores, roc_chart, truth_space_table
from dataengineeringutils3.s3 import write_local_file_to_s3

from constants import get_paths_from_job_path

from custom_logger import get_custom_logger


sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
spark = glue_context.spark_session

args = getResolvedOptions(
    sys.argv,
    [
        "job_path",
        "snapshot_date",
        "commit_hash",
        "trial_run",
        "version",
        "job_name_override",
    ],
)

trial_run = args["trial_run"] == "true"

PARALLELISM = 200
spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)


custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")


# Output paths can be derived from the path
job_name_override = args["job_name_override"]
output_paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    labelling_exercise="qa_2021",
    job_name=job_name_override,
)


# for k, v in output_paths.items():
#     custom_log.info(f"{k:<50} {v}")


# Create labelled data from person id
SOURCE_NODES_PATH = output_paths["source_nodes_path"]
SOURCE_NODES_PATH = SOURCE_NODES_PATH.replace(job_name_override, "basic")

df_source = spark.read.parquet(SOURCE_NODES_PATH)

edges_path = output_paths["edges_path"]
df_edges = spark.read.parquet(edges_path)
df_source.createOrReplaceTempView("df_source")
df_edges.createOrReplaceTempView("df_edges")

sql = """
select
    df_l.unique_id as unique_id_l,
    df_r.unique_id as unique_id_r,
    1.0 as clerical_match_score

from df_source as df_l
left join df_source as df_r

on df_l.cluster = df_r.cluster

where df_l.unique_id < df_r.unique_id

union all

select
    df_edges.unique_id_l as unique_id_l,
    df_edges.unique_id_r as unique_id_r,
    0.0 as clerical_match_score
    from df_edges
    where  cluster_l != cluster_r
"""
df_labels = spark.sql(sql)
df_labels.persist()

# Join labels to each df_e


df_e_with_labels_v2 = labels_with_splink_scores(
    df_labels,
    df_edges,
    "unique_id",
    spark,
    retain_all_cols=True,
)


df_e_with_labels_v2 = df_e_with_labels_v2.repartition(10)
df_e_with_labels_v2 = df_e_with_labels_v2.withColumn(
    "commit_hash", f.lit(args["commit_hash"])
)


output_path = output_paths["labels_with_scores_path"]


df_e_with_labels_v2.write.mode("overwrite").parquet(output_path)

custom_log.info(f"outputting scores with labels to to {output_path}")

PARALLELISM = 20
spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)


df_e_with_labels_v2 = spark.read.parquet(output_path)

cols = df_e_with_labels_v2.columns
if "df_e__tf_adjusted_match_weight" in cols:
    weight_col = "df_e__tf_adjusted_match_weight"
else:
    weight_col = "df_e__match_weight"

df_e_with_labels_v2 = df_e_with_labels_v2.withColumn(
    "bf_temp", f.expr(f"round({weight_col}/2, 1)*2")
)
df_e_with_labels_v2 = df_e_with_labels_v2.withColumn(
    "tf_adjusted_match_prob", f.expr("power(2, bf_temp)/(1+power(2,bf_temp))")
)
df_e_with_labels_v2 = df_e_with_labels_v2.drop("bf_temp")

truth_space_table = truth_space_table(df_e_with_labels_v2, spark)

truth_space_table_path = os.path.join(
    output_paths["charts_directory_path"], "truth_space_table"
)
truth_space_table = truth_space_table.repartition(1)
truth_space_table.write.mode("overwrite").parquet(truth_space_table_path)
