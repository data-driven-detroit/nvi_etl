from pathlib import Path
import pandas as pd
from sqlalchemy import text

from nvi_etl import make_engine_for
from nvi_etl.schema import NVIContextValueTable

WORKING_DIR = Path(__file__).parent

def main():
    db = make_engine_for("nvi_test") # con

    query = Path(WORKING_DIR / "sql" / "calculate_pop_density.sql").read_text()

    frame = pd.read_sql(query, db)

    validated = NVIContextValueTable.validate(frame)

    validated.to_sql("context_value", db, schema=None, if_exists='append')

if __name__ == "__main__":
    main()