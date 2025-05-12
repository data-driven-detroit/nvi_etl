from nvi_etl import setup_logging

from extract import extract_mischooldata
from transform import transform_mischooldata
from load import load_mischooldata


def main():
    logger = setup_logging()


    extract_mischooldata(logger)
    transform_mischooldata(logger)
    load_mischooldata(logger)

if __name__ == "__main__":
    main()