"""Import all task modules to trigger @task registration."""

from nvi_etl.tasks import acs  # noqa: F401
from nvi_etl.tasks import acs_v2  # noqa: F401
from nvi_etl.tasks import evictions  # noqa: F401
from nvi_etl.tasks import geography  # noqa: F401
from nvi_etl.tasks import ipds  # noqa: F401
from nvi_etl.tasks import mischooldata  # noqa: F401
from nvi_etl.tasks import msc  # noqa: F401
from nvi_etl.tasks import primary_survey  # noqa: F401
