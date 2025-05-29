"""
CREATE CONTEXT FILE:

python ./gathering.py
"""

"""
COPY CONTEXT FILE TO SERVER
scp 

"""

"""
NEED TO RUN THIS ON SERVER PSQL:

-- Get rid of everything
DELETE FROM context_value;

-- Push everything up
\copy context_value FROM '/home/mike/nvi_context_values_20250521.csv' WITH (FORMAT csv, HEADER);
"""