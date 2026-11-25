import os
import tempfile
import pytest

_tmp = tempfile.mkdtemp(prefix="lb_tests_")
os.environ["LB_DATA"] = _tmp
os.environ["LB_SCALE"] = "0.1"
os.environ["LB_FORMAT"] = "parquet"


@pytest.fixture(scope="session")
def pipeline():
    from lakehouse_bridge import data_gen
    from lakehouse_bridge.pipelines import batch, streaming
    from lakehouse_bridge.migration import migrate
    from lakehouse_bridge.governance import security, quality
    out = {"gen": data_gen.generate()}
    out["batch"] = batch.run()
    out["stream_rows"] = streaming.run()
    out["hourly_rows"] = streaming.build_hourly()
    out["recon"] = migrate.run()
    out["roles"] = security.build_role_databases()
    out["quality"] = quality.run()
    return out
