import datetime
import pandas as pd
from nvi_survey import create_nvi_survey


def save_excel_sheets(tables: list[tuple[str, pd.DataFrame]], filename: str):
    with pd.ExcelWriter(
        filename,
        engine="xlsxwriter"
    ) as w:
        for i, (label, table) in enumerate(tables):
            table.to_excel(w, sheet_name=f"{i}-{label}"[:30])


def generate_digital_equity_report(filepath):
    survey = create_nvi_survey(filepath)

    answer_groups = survey.answer_key[
        survey.answer_key["included_in_arpa"]
    ][[
        "group", 
        "question", 
        "answer", 
        "response_type"
    ]].groupby(["group", "response_type"])

    tables = []
    for (group, response_type), group_rows in answer_groups:
        if response_type == "GROUPED-SINGLE":
            question_groups = group_rows.groupby("question")
            for q, _ in question_groups:
                citywide = survey.tabulate_single_question("citywide", q, group)
                districts = survey.tabulate_single_question("district", q, group)
                zones = survey.tabulate_single_question("zone", q, group)

                all_geos = pd.concat([citywide, districts, zones])

                tables.append((q, all_geos))

        elif response_type == "MULTI-SELECT":
            citywide = survey.tabulate_multiselect("citywide", ..., group)
            districts = survey.tabulate_multiselect("district", ..., group)
            zones = survey.tabulate_multiselect("zone", ..., group)

            all_geos = pd.concat([citywide, districts, zones])

            tables.append((group, all_geos))

        elif response_type == "GRID-OF-DEATH":
            citywide = survey.tabulate_grid_of_death("citywide", group)
            districts = survey.tabulate_grid_of_death("district", group)
            zones = survey.tabulate_grid_of_death("zone", group)

            all_geos = pd.concat([citywide, districts, zones])

            tables.append((group, all_geos))

    save_excel_sheets(
        tables,
        "C:\\Users\\mike\\Desktop\\digital_equity_questions_20250512.xlsx",
    )


def compile_simple_nvi_dataset(filepath):
    survey = create_nvi_survey(filepath)
    today = datetime.date.today().strftime("%Y%m%d")

    indicator_groups = (
        survey.answer_key[~survey.answer_key["indicator_db_id"].isna()]
        .groupby(["indicator_db_id", "response_type"]) # indicators can only have one response type (possible error though)
    )

    tables = []

    for (indicator, response_type), group in indicator_groups:
        if response_type in {"SINGLE", "GROUPED-SINGLE"}:
            tables.append(survey.compile_single_response_indicator(indicator, "district")["percentage"].rename(indicator))

        elif response_type == "MULTI-SELECT":
            tables.append(survey.compile_multi_response_indicator(indicator, "district")["percentage"].rename(indicator))

    pd.concat(tables, axis=1).rename(columns=survey.indicator_key).to_excel(
        f"nvi_survey_indicator_2024_2026_2026_{today}.xlsx"
    )

if __name__ == "__main__":
    generate_digital_equity_report("C:\\Users\\mike\\Desktop\\2_responsibilities\\nvi_etl\\primary_survey\\v2024\\output\\nvi_2024_analysis_source_2014_2022_20250624.csv")