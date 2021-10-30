from splink.case_statements import sql_gen_case_stmt_jaro_4


from postcode_location import expr_distance_in_km


def dob_case_statement_leven(dob_colname, leven_distance=1):
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


postcode_custom_expression = f"""
case
when (postcode_l is null or postcode_r is null) then -1
when postcode_l = postcode_r then 4
when  ({expr_distance_in_km('lat_lng')} < 5) then 3
when  ({expr_distance_in_km('lat_lng')} < 50)  then 2
when  ({expr_distance_in_km('lat_lng')} < 150)  then 1
else 0
end
"""


settings = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": ["l.postcode = r.postcode"],
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
            "case_expression": dob_case_statement_leven("dob"),
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "custom_name": "postcode",
            "custom_columns_used": [
                "postcode",
                "lat_lng",
                "birth_place",
            ],
            "case_expression": postcode_custom_expression,
            "num_levels": 5,
            "term_frequency_adjustments": True,
        },
    ],
    "additional_columns_to_retain": ["cluster", "source_dataset"],
}
