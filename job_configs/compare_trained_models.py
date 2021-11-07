import os

try:
    ROLE = os.environ["GLUE_ROLE"]
except KeyError:
    raise ValueError("You must provide a role name")

compare_trained_models = {
    "model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/00_estimate_u": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/01_block_postcode_dob": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/02_block_name": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 5,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "model_training/person/uk_citizens_max_groupsize_20/compare_trained_models/03_combine_blocks": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 2,
        "version": "v01",
    },
    "edge_generation/person/uk_citizens_max_groupsize_20/compare_trained_models": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 8,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
    "cluster_generation/person/uk_citizens_max_groupsize_20/compare_trained_models/01_cluster": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 6,
        "version": "v01",
        "remove_python_modules": ["splink"],
        "additional_job_args": {
            "--conf": (
                "spark.jars.packages=graphframes:graphframes:0.8.2-spark3.1-s_2.12"
            )
        },
    },
    "qa/compute_accuracy/person/uk_citizens_max_groupsize_20/compare_trained_models": {
        "role": ROLE,
        "snapshot_date": "2021-01-01",
        "allocated_capacity": 8,
        "version": "v01",
        "remove_python_modules": ["splink"],
    },
}
