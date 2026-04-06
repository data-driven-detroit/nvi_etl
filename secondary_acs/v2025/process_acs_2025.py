from nvi_etl import setup_logging
from extract import extract
from transform import transform
from load import load_acs


def main():
    logger = setup_logging()

    extract(logger)
    transform(logger)
    load_acs(logger)


if __name__ == "__main__":
    main()