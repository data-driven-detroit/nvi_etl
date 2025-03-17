from nvi_etl.schema import NVIValueTable
import pandas as pd


def load_data(logger):
    logger.info("Loading data...")
    
    # Open file
    file = ...

    # Check against value table schema
    
    NVIValueTable.validate(file) # type: ignore
    
    # Check table for conflicting rows
   
    # Fail if any rows are conflicting

    # Instruct user to remove conflicts
    
    # TODO: Instead of failing for conflicts, do an 'upsert':
    #   - Remove conflicting rows from table
    #   - Insert all new rows after removing conflicts

    logger.info("Data loaded successfully!")
