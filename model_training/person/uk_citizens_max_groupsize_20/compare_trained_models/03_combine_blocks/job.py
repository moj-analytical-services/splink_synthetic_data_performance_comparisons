import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from dataengineeringutils3.s3 import (
    read_json_from_s3,
    write_json_to_s3,
    write_local_file_to_s3,
)

from constants import get_paths_from_job_path
from splink.model import load_model_from_dict
from splink.combine_models import ModelCombiner, combine_cc_estimates
from splink.settings import Settings

from custom_logger import get_custom_logger

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


pc_dob_path = paths["training_models_path"].replace(
    "03_combine_blocks",
    "01_block_postcode_dob",
)

pc_dob_path = os.path.join(pc_dob_path, "saved_model_final.json")

name_path = paths["training_models_path"].replace(
    "03_combine_blocks",
    "02_block_name",
)

name_path = os.path.join(name_path, "saved_model_final.json")


#  postcode model
pc_dob_json = read_json_from_s3(pc_dob_path)
pc_dob_model = load_model_from_dict(pc_dob_json)

# name model
name_json = read_json_from_s3(name_path)
name_model = load_model_from_dict(name_json)


cc_pc = name_model.current_settings_obj.get_comparison_column("postcode")
cc_dob = name_model.current_settings_obj.get_comparison_column("dob")


# Same thing from the surname/postcode blocking job
cc_sn = pc_dob_model.current_settings_obj.get_comparison_column("surname_std")
cc_fn = pc_dob_model.current_settings_obj.get_comparison_column("forename1_std")


# To get out
pc_dob_dict = {
    "name": "postcode_dob",
    "model": pc_dob_model,
    "comparison_columns_for_global_lambda": [cc_pc, cc_dob],
}


name_dict = {
    "name": "name",
    "model": name_model,
    "comparison_columns_for_global_lambda": [
        cc_sn,
        cc_fn,
    ],
}


mc = ModelCombiner([pc_dob_dict, name_dict])

global_settings_dict = mc.get_combined_settings_dict()

# Now we have global settings, we just need a set of blocking rules to produce potential matches

global_settings_dict["blocking_rules"] = [
    "l.postcode = r.postcode and l.dob_year = r.dob_year",
    "l.postcode = r.postcode and l.dob_month = r.dob_month",
    "l.postcode = r.postcode and l.dob_day = r.dob_day",
    "l.postcode = r.postcode and l.forename1_dm = r.forename1_dm",
    "l.postcode = r.postcode and l.forename2_dm = r.forename2_dm",
    "l.postcode = r.postcode and l.surname_dm = r.surname_dm",
    "l.dob = r.dob and l.outward_postcode_std = r.outward_postcode_std",
    "l.dob = r.dob and l.inward_postcode_std = r.inward_postcode_std",
    "l.dob = r.dob and l.forename1_dm = r.forename1_dm",
    "l.dob = r.dob and l.forename2_dm = r.forename2_dm",
    "l.dob = r.dob and l.surname_dm = r.surname_dm",
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.dob_year = r.dob_year",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.dob_year = r.dob_year",
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.dob_month = r.dob_month",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.dob_month = r.dob_month",
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.dob_day = r.dob_day",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.dob_day = r.dob_day",
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.inward_postcode_std = r.inward_postcode_std",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.inward_postcode_std = r.inward_postcode_std",
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.outward_postcode_std = r.outward_postcode_std",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.outward_postcode_std = r.outward_postcode_std",
    "l.surname_std = r.surname_std and l.outward_postcode_std = r.outward_postcode_std",
    "l.surname_std = r.surname_std and l.inward_postcode_std = r.inward_postcode_std",
    "l.forename1_std = r.forename1_std and l.forename2_std = r.forename2_std",
    # "l.cluster = r.cluster",
]

path = os.path.join(paths["training_combined_model_path"], "combined_settings.json")
write_json_to_s3(global_settings_dict, path)

chart = mc.comparison_chart()


chart_name = "model_combiner_comparison_chart.html"
chart.save(chart_name)

charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)


s = Settings(global_settings_dict)
chart = s.bayes_factor_chart()
chart_name = "combined_bayes_factor.html"
chart.save(chart_name)

charts_dir = paths["charts_directory_path"]
path = os.path.join(charts_dir, chart_name)
write_local_file_to_s3(chart_name, path, overwrite=True)
