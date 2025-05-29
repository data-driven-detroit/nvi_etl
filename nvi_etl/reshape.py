import pandas as pd


def elongate(wide):
    """
    This funciton is finicky to call, but if you follow the column-name
    protocol you can just apply it directly.
    """

    # TODO Add some warning for non-reshaped functions

    return (
        pd.wide_to_long(
            wide,
            stubnames=[
                "count", 
                "universe",
                "percentage",
                "rate",
                "per",
                "dollars",
                "index"
            ],
            i=["location_id", "year"],
            j="indicator",
            sep="_",
            suffix=".*",
        )
        .reset_index()
        .rename(columns={"per": "rate_per"})
    )