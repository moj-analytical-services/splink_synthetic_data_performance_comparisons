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
from splink.maximisation_step import run_maximisation_step
from splink.model import Model
from splink import Splink
from pyspark.sql.functions import lit


from splink_settings import (
    model_01_two_levels,
    model_02_fuzzy_simple,
    model_03_fuzzy_complex,
    model_04_fuzzy_complex_and_tf,
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

custom_log.info(args)


job_name_override = args["job_name_override"]
# Output paths can be derived from the path
paths = get_paths_from_job_path(
    args["job_path"],
    args["snapshot_date"],
    args["version"],
    trial_run=trial_run,
    job_name=job_name_override,
)

for k, v in paths.items():
    custom_log.info(f"{k:<50} {v}")

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


if job_name_override == "model_01_two_levels":
    settings = model_01_two_levels
    custom_log.info("Using settings: model_01_two_levels")
if job_name_override == "model_02_fuzzy_simple":
    settings = model_02_fuzzy_simple
    custom_log.info("Using settings: model_02_fuzzy_simple")
if job_name_override == "model_03_fuzzy_complex":
    settings = model_03_fuzzy_complex
    custom_log.info("Using settings: model_03_fuzzy_complex")
if job_name_override == "model_04_fuzzy_complex_and_tf":
    settings = model_04_fuzzy_complex_and_tf
    custom_log.info("Using settings: model_04_fuzzy_complex_and_tf")


# Estimate u params for all columns from cartesian product
settings_with_u_dict = estimate_u_values(
    settings,
    person_standardised_nodes,
    spark,
    target_rows=target_rows,
    fix_u_probabilities=True,
)

# Skip tf adjustments, not needed to estimate m
for c in settings["comparison_columns"]:
    c["term_frequency_adjustments"] = False

# We're blocking on PNC ID, so we need to remove that comparison column
settings_obj = Settings(settings)


linker = Splink(settings_obj.settings_dict, person_standardised_nodes, spark)


df_e = linker.manually_apply_fellegi_sunter_weights()

# Set all matche probabilities to 1
df_e = df_e.withColumn("match_probability", lit(1.0))

model = Model(settings_obj.settings_dict, spark)
run_maximisation_step(df_e, model, spark)
settings_with_m_dict = model.current_settings_obj.settings_dict

# We want to add m probabilities from these estimates to the settings_with_u object
settings_with_u_obj = Settings(settings_with_u_dict)

settings_with_u_obj.overwrite_m_u_probs_from_other_settings_dict(
    settings_with_m_dict, overwrite_u=False
)


s3_output_path = os.path.join(
    paths["training_combined_model_path"], "final_settings.json"
)


write_json_to_s3(settings_with_u_dict, s3_output_path)

custom_log.info(f"Written settings dict to to {s3_output_path}")
