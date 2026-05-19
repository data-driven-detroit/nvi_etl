from nvi_etl import setup_logging
from extract import extract
from transform import transform

def main():
    logger = setup_logging()

    extract(logger)
    transform(logger)


if __name__ == "__main__":
    main()