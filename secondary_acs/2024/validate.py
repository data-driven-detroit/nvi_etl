from nvi_etl.schema import NVIValueTable


def validate_acs_to_nvi_schema(logger):
    """
    Make sure the ACS dataset aligns with the output schema
    """
    logger.info("Validating ACS indicators for NVI schema.")

    # TODO: Open file from 'load'
    file = ...

    # Validate file
    NVIValueTable.validate(file)
    
    # If this breaks, it throws a helpful error
    logger.info("SUCCESS!")

