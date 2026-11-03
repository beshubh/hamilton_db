import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.test.runner import DiscoverRunner

REPO = Path(settings.BASE_DIR).parent.parent


class HamiltonTestRunner(DiscoverRunner):
    """Runs the tests against a hamilton-server of their own.

    A server serves exactly one database, and the tests empty every table
    between cases, so they get a fresh server on a free port rather than
    the one in DATABASES.
    """

    def setup_databases(self, **kwargs):
        binary = Path(
            os.environ.get("HAMILTON_SERVER", REPO / "target/release/hamilton-server")
        )
        if not binary.is_file():
            raise ImproperlyConfigured(
                f"{binary} not found: run `cargo build --release -p hamilton-server` "
                "or point HAMILTON_SERVER at the binary"
            )
        self.data_dir = tempfile.mkdtemp(prefix="hamilton-test-")
        self.server = subprocess.Popen(
            [binary, "--port", "0", os.path.join(self.data_dir, "test.db")],
            stderr=subprocess.PIPE,
            text=True,
        )
        line = self.server.stderr.readline()
        match = re.search(r"listening on [\d.]+:(\d+)", line)
        if not match:
            self.stop_server()
            raise RuntimeError(f"hamilton-server didn't start: {line!r}")
        connections["default"].settings_dict["PORT"] = match.group(1)
        return super().setup_databases(**kwargs)

    def teardown_databases(self, old_config, **kwargs):
        try:
            super().teardown_databases(old_config, **kwargs)
        finally:
            self.stop_server()

    def stop_server(self):
        self.server.terminate()
        self.server.wait(timeout=10)
        shutil.rmtree(self.data_dir, ignore_errors=True)
