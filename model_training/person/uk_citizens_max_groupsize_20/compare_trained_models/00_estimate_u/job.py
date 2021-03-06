import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import os


from dataengineeringutils3.s3 import write_json_to_s3

from constants import get_paths_from_job_path


from lineage import curried_blocked_comparisons_to_s3
from custom_logger import get_custom_logger
from spark_config import add_udfs

from splink.estimate import estimate_u_values

from splink.settings import Settings


from splink_settings import (
    model_01_two_levels_trained,
    model_02_fuzzy_simple_trained,
    model_03_fuzzy_complex_trained,
    model_04_fuzzy_complex_and_tf_trained,
    model_05_fuzzy_complex_and_tf_weights_trained,
)

sc = SparkContext()
glue_context = GlueContext(sc)
glue_logger = glue_context.get_logger()
spark = glue_context.spark_session

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "job_path",
        "snapshot_date",
        "commit_hash",
        "trial_run",
        "version",
        "job_name_override",
    ],
)

trial_run = args["trial_run"] == "true"
if trial_run:
    PARALLELISM = 8
else:
    PARALLELISM = 50

spark.conf.set("spark.sql.shuffle.partitions", PARALLELISM)
spark.conf.set("spark.default.parallelism", PARALLELISM)

add_udfs(spark)


# Set up a custom logger than outputs to its own stream, grouped within the job run id
# to separate out custom logs from general spark logs
custom_log = get_custom_logger(args["JOB_RUN_ID"])

custom_log.info(f"Snapshot date is {args['snapshot_date']}")

# custom_log.info(args)


job_name_override = args["job_name_override"]
# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    job_name=job_name_override,
)

# for k, v in paths.items():
#     custom_log.info(f"{k:<50} {v}")

PERSON_STANDARDISED_NODES_PATH = paths["standardised_nodes_path"]
PERSON_STANDARDISED_NODES_PATH = PERSON_STANDARDISED_NODES_PATH.replace(
    job_name_override, "basic"
)

custom_log.info(f"PERSON_STANDARDISED_NODES_PATH: {PERSON_STANDARDISED_NODES_PATH}")

person_standardised_nodes = spark.read.parquet(PERSON_STANDARDISED_NODES_PATH)

if trial_run:
    target_rows = 1e6
else:
    target_rows = 1e8


blocked_comparisons_to_s3 = curried_blocked_comparisons_to_s3(
    paths, custom_log, PARALLELISM
)


if job_name_override == "model_01_two_levels_trained":
    settings = model_01_two_levels_trained
    custom_log.info("Using settings: model_01_two_levels_trained")
if job_name_override == "model_02_fuzzy_simple_trained":
    settings = model_02_fuzzy_simple_trained
    custom_log.info("Using settings: model_02_fuzzy_simple_trained")
if job_name_override == "model_03_fuzzy_complex_trained":
    settings = model_03_fuzzy_complex_trained
    custom_log.info("Using settings: model_03_fuzzy_complex_trained")
if job_name_override == "model_04_fuzzy_complex_and_tf_trained":
    settings = model_04_fuzzy_complex_and_tf_trained
    custom_log.info("Using settings: model_04_fuzzy_complex_and_tf_trained")
if job_name_override == "model_05_fuzzy_complex_and_tf_weights_trained":
    settings = model_05_fuzzy_complex_and_tf_weights_trained
    custom_log.info("Using settings: model_05_fuzzy_complex_and_tf_weights_trained")


# Estimate u params for all columns from cartesian product
settings_with_u_dict = estimate_u_values(
    settings,
    person_standardised_nodes,
    spark,
    target_rows=target_rows,
    fix_u_probabilities=True,
)


settings_obj = Settings(settings)


count_for_log = person_standardised_nodes.count()
custom_log.info(f"The count of source nodes is {count_for_log:,.0f}")

blocked_comparisons_to_s3 = curried_blocked_comparisons_to_s3(
    paths, custom_log, PARALLELISM
)


# Estimate u params for all columns from cartesian product
settings_with_u_dict = estimate_u_values(
    settings,
    person_standardised_nodes,
    spark,
    target_rows=target_rows,
    fix_u_probabilities=True,
)


s3_output_path = os.path.join(paths["training_models_path"], "settings_with_u.json")
write_json_to_s3(settings_with_u_dict, s3_output_path)

custom_log.info(f"Written settings dict to to {s3_output_path}")