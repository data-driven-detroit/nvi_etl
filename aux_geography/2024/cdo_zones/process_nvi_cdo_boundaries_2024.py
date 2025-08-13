from transform import transform_cdo_boundaries
from load import load_cdo_boundaries

"""
These are required to run before the rest of the scripts in the file, because
the aggregations are dependent on having the appropriate zones.
"""


# Council Districts workflow
transform_cdo_boundaries()
load_cdo_boundaries()
