from pathlib import Path
from tablecensus.assemble import assemble_from

WORKING_DIR = Path(__file__).resolve().parent


def extract(logger):
    logger.info("Assembling NVI ACS part 2")

    output_path = WORKING_DIR / "input" / "nvi_2024_acs.csv"

    if output_path.exists():
        logger.info(f"Data already pulled, delete file in 'input' to pull again.")
        return

    assembled = assemble_from(WORKING_DIR / "conf" / "definitions.xlsx")
    assembled.to_csv(output_path, index=False)