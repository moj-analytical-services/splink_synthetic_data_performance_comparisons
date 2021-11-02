from splink.case_statements import sql_gen_case_stmt_jaro_4, sql_gen_case_stmt_jaro_3


from postcode_location import expr_distance_in_km


def dob_case_statement_leven_4(dob_colname, leven_distance=1):
    # It's often the case that too many dates of birth are on the first of january
    # typically because when exact dob is unavailable they round to nearest year
    return f"""
    case
    when {dob_colname}_l is null or {dob_colname}_r is null then -1
    when {dob_colname}_l = {dob_colname}_r  and substr({dob_colname}_l, -5) = '01-01'  then 2
    when {dob_colname}_l = {dob_colname}_r  then 3
    when levenshtein({dob_colname}_l, {dob_colname}_r) <= {leven_distance} then 1
    else 0 end
    """


def dob_case_statement_leven_3(dob_colname, leven_distance=1):
    # It's often the case that too many dates of birth are on the first of january
    # typically because when exact dob is unavailable they round to nearest year
    return f"""
    case
    when {dob_colname}_l is null or {dob_colname}_r is null then -1
    when {dob_colname}_l = {dob_colname}_r  then 2
    when levenshtein({dob_colname}_l, {dob_colname}_r) <= {leven_distance} then 1
    else 0 end
    """


postcode_custom_expression_5 = f"""
case
when (postcode_l is null or postcode_r is null) then -1
when postcode_l = postcode_r then 4
when  ({expr_distance_in_km('lat_lng')} < 5) then 3
when  ({expr_distance_in_km('lat_lng')} < 50)  then 2
when  ({expr_distance_in_km('lat_lng')} < 150)  then 1
else 0
end
"""

postcode_custom_expression_3 = """
case
when (postcode_l is null or postcode_r is null) then -1
when postcode_l = postcode_r then 2
when levenshtein(postcode_l, postcode_r) <= 1 then 1
else 0
end
"""

blocking_rules = [
    "l.outward_postcode_std = r.outward_postcode_std and l.dob = r.dob",
    "l.postcode = r.postcode and l.dob_year = r.dob_year",
    "l.postcode = r.postcode and l.dob_month = r.dob_month",
    "l.postcode = r.postcode and l.dob_day = r.dob_day",
    "l.forename1_dm = r.forename1_dm and l.occupation = r.occupation and l.dob_year = r.dob_year",
    "l.forename1_dm = r.forename1_dm and l.surname_dm = r.surname_dm and l.dob_year = r.dob_year",
    "l.cluster = r.cluster",
]

model_01_two_levels = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": blocking_rules,
    "comparison_columns": [
        {
            "col_name": "surname_std",
        },
        {
            "col_name": "forename1_std",
        },
        {
            "col_name": "forename2_std",
        },
        {
            "col_name": "occupation",
        },
        {
            "col_name": "dob",
        },
        {
            "col_name": "postcode",
        },
    ],
    "additional_columns_to_retain": ["cluster"],
}

model_02_fuzzy_simple = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": blocking_rules,
    "comparison_columns": [
        {
            "col_name": "surname_std",
            "case_expression": sql_gen_case_stmt_jaro_3("surname_std"),
            "num_levels": 3,
        },
        {
            "col_name": "forename1_std",
            "case_expression": sql_gen_case_stmt_jaro_3("forename1_std"),
            "num_levels": 3,
        },
        {"col_name": "forename2_std"},
        {"col_name": "occupation"},
        {
            "col_name": "dob",
            "case_expression": dob_case_statement_leven_3("dob"),
            "num_levels": 3,
        },
        {
            "col_name": "postcode",
            "case_expression": postcode_custom_expression_3,
            "num_levels": 3,
        },
    ],
    "additional_columns_to_retain": ["cluster"],
}

model_03_fuzzy_complex = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": blocking_rules,
    "comparison_columns": [
        {
            "col_name": "surname_std",
            "case_expression": sql_gen_case_stmt_jaro_4("surname_std"),
            "num_levels": 4,
        },
        {
            "col_name": "forename1_std",
            "case_expression": sql_gen_case_stmt_jaro_4("forename1_std"),
            "num_levels": 4,
        },
        {
            "col_name": "forename2_std",
            "case_expression": sql_gen_case_stmt_jaro_4("forename2_std"),
            "num_levels": 4,
        },
        {
            "col_name": "occupation",
            "num_levels": 2,
        },
        {
            "col_name": "dob",
            "case_expression": dob_case_statement_leven_4("dob"),
            "num_levels": 4,
        },
        {
            "custom_name": "postcode",
            "custom_columns_used": [
                "postcode",
                "lat_lng",
                "birth_place",
            ],
            "case_expression": postcode_custom_expression_5,
            "num_levels": 5,
        },
    ],
    "additional_columns_to_retain": ["cluster"],
}


model_04_fuzzy_complex_and_tf = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": blocking_rules,
    "comparison_columns": [
        {
            "col_name": "surname_std",
            "case_expression": sql_gen_case_stmt_jaro_4("surname_std"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "col_name": "forename1_std",
            "case_expression": sql_gen_case_stmt_jaro_4("forename1_std"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "col_name": "forename2_std",
            "case_expression": sql_gen_case_stmt_jaro_4("forename2_std"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "col_name": "occupation",
            "num_levels": 2,
            "term_frequency_adjustments": True,
        },
        {
            "col_name": "dob",
            "case_expression": dob_case_statement_leven_4("dob"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "custom_name": "postcode",
            "custom_columns_used": [
                "postcode",
                "lat_lng",
            ],
            "case_expression": postcode_custom_expression_5,
            "num_levels": 5,
            "term_frequency_adjustments": True,
        },
    ],
    "additional_columns_to_retain": ["cluster"],
}

model_05_fuzzy_complex_and_tf_weights = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": blocking_rules,
    "comparison_columns": [
        {
            "col_name": "surname_std",
            "case_expression": sql_gen_case_stmt_jaro_4("surname_std"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
            "tf_adjustment_weights": [0, 0.2, 0.5, 1],
        },
        {
            "col_name": "forename1_std",
            "case_expression": sql_gen_case_stmt_jaro_4("forename1_std"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
            "tf_adjustment_weights": [0, 0.2, 0.5, 1],
        },
        {
            "col_name": "forename2_std",
            "case_expression": sql_gen_case_stmt_jaro_4("forename2_std"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
            "tf_adjustment_weights": [0, 0.2, 0.5, 1],
        },
        {
            "col_name": "occupation",
            "num_levels": 2,
            "term_frequency_adjustments": True,
        },
        {
            "col_name": "dob",
            "case_expression": dob_case_statement_leven_4("dob"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
            "tf_adjustment_weights": [0, 0.2, 0.5, 1],
        },
        {
            "custom_name": "postcode",
            "custom_columns_used": [
                "postcode",
                "lat_lng",
            ],
            "case_expression": postcode_custom_expression_5,
            "num_levels": 5,
            "term_frequency_adjustments": True,
            "tf_adjustment_weights": [0, 0, 0.2, 0.5, 1],
        },
    ],
    "additional_columns_to_retain": ["cluster"],
}
