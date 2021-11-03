#!/bin/bash
set -e
set -o pipefail


# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_models/00_estimate_m_u --job_name_override=model_01_two_levels
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_01_two_levels
# python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_01_two_levels

# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_models/00_estimate_m_u --job_name_override=model_02_fuzzy_simple
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_02_fuzzy_simple
# python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_02_fuzzy_simple


# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_models/00_estimate_m_u --job_name_override=model_03_fuzzy_complex
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_03_fuzzy_complex
# python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_03_fuzzy_complex

# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_models/00_estimate_m_u --job_name_override=model_04_fuzzy_complex_and_tf
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_04_fuzzy_complex_and_tf
# python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_04_fuzzy_complex_and_tf

# python jobrunner.py --job_path model_training/person/uk_citizens_max_groupsize_20/compare_models/00_estimate_m_u --job_name_override=model_05_fuzzy_complex_and_tf_weights
# python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_05_fuzzy_complex_and_tf_weights
# python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_05_fuzzy_complex_and_tf_weights

python jobrunner.py --job_path edge_generation/person/uk_citizens_max_groupsize_20/compare_models_rules_based --job_name_override=model_00_rules_based
python jobrunner.py --job_path qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_models --job_name_override=model_00_rules_based

