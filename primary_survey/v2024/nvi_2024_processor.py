from nvi_etl import setup_logging
from extract_data import extract_data
from transform_data import transform_data
from load_data import load_data


def main():
    logger = setup_logging()


    extract_data(logger)
    transform_data(logger, cdo_aggregate=True)
    # transform_data(logger, survey_year=2023)
    # load_data(logger)


if __name__ == "__main__":
    main()