import configparser
import shutil

# This function copies the data from the data_extract_path to the local input directory for processing
def extract_data(logger):
    logger.info('Extracting data...')

    parser = configparser.ConfigParser()
    parser.read('./conf/.conf')
    data_extract_path = parser.get('nvi_2024_config', 'data_extract_path')
    shutil.copyfile(data_extract_path, './input/data.csv')

    logger.info('Data extracted successfully!')
