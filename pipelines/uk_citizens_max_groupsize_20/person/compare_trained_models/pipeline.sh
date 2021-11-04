#!/bin/bash
set -e
set -o pipefail


# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/00_estimate_u --job_name_override=model_01_two_levels_trained
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/01_block_postcode_dob --job_name_override=model_01_two_levels_trained
# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/02_block_name --job_name_override=model_01_two_levels_trained
python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/03_combine_blocks --job_name_override=model_01_two_levels_trained


