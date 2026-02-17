from nvi_etl import setup_logging
from extract_data import extract_data
from transform_data import transform_data
from load_data import load_data


def main():
    logger = setup_logging()


    """
    Setting the cdo_aggregate to true will aggregate the cdo profiles, otherwise
    it will aggregate to neighborhood zone, council district, and citywide.

    If you want to create a new cdo file you must also delete any file named 
    'primary_survey_tall_2024_cdos.csv' From the output directory.
    """

    extract_data(logger)
    transform_data(logger, cdo_aggregate=False)
    # load_data(logger)


if __name__ == "__main__":
    main()