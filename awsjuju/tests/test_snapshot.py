import logging
import time

from awsjuju.lock import Lock
from awsjuju.services.snapshot import SnapshotRunner
from awsjuju.tests.common import EC2Base


class Options(dict):

    def __getattr__(self, key):
        return self.get(key)


class SnapshotTest(EC2Base):

    def setUp(self):
        super(SnapshotTest, self).setUp()
        self.lock = Lock()
        self.runner = SnapshotRunner()
        self.output = self.capture_logging(
            "aws-snapshot", level=logging.DEBUG)

    def reset_output(self):
        self.output.seek(0, 0)
        self.output.buf = ''

    def create_volume(self, size, zone):
        vol = self.ec2.create_volume(size, zone)
        while True:
            if vol.status == 'READY':
                time.sleep(4)
            vol.update(True)

    def test_register(self):
        # Try with non existant instance
        options = Options(instance_id="i-abc", app="abc", unit="foobar")
        self.assertFalse(self.runner.register(options))
        self.assertIn("Invalid instance id", self.output.getvalue())

        # Try with a second volume attached
        vol = self.create_volume(5, self.instances[0].placement)
        self.ec2.attach_volume(vol.id, self.instances[0].id, "/dev/sdf")
        options = Options(
            instance_id=self.instances[0].id, app="abc", unit="foobar")
        self.assertFalse(self.runner.register(options))

        # Try normally.
        options = Options(
            instance_id=self.instances[1].id, app="abc", unit="foobar")
        self.assertTrue(self.runner.register(options))

    def test_get_volume(self):
        pass

    def test_get_snapshot_instances(self):
        # Check with non existant record
        item = self.instance_db.new_item(
            "someapp", "i-fooar", {})
        item.save()

    def test_run_period(self):
        pass
