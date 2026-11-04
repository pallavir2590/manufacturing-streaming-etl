import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mfg_etl.local_spark import get_local_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = get_local_spark("mfg-etl-tests")
    yield session
    session.stop()
