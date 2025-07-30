from pathlib import Path
import pandas as pd

"""
I created this test while figuring out some issues with universes. It checks 
that all questions that make up an indicator have the same universe as the 
indicator itself. This is kind of a leaky abstraction, so among multi-questions 
indicators there are a few that don't match perfectly. We didn't have a
clear idea of how to handle the nulls anyway.
"""



WORKING_DIR = Path(__file__).parent


def test_primary(logger):
    frame = pd.read_csv(WORKING_DIR / "output" / "primary_survey_tall_2024.csv")

    errors = 0
    for g, rows in frame.groupby("indicator_id"):
        detroit = rows[rows["location_id"] == 1]
        if not (detroit["universe"] == detroit.iloc[0]["universe"]).all():
            print(f"Indicator ID {g} failing to match universe.")
            print(detroit[["survey_question_id", "survey_question_option_id", "count", "universe", "percentage"]])
    
            errors += 1

    print(f"{errors} total errors.")


if __name__ == "__main__":

    from nvi_etl import setup_logging

    logger = setup_logging()
    test_primary(logger)