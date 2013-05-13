from boto import dynamodb

import boto.exception
import logging
import os
import uuid
import time
import StringIO
import sys

from unittest2 import TestCase

log = logging.getLogger("awsjuju.test")


def setup_test_log(log):
    stream = logging.StreamHandler(sys.stderr)
    stream.setLevel(logging.DEBUG)
    stream.setFormatter(
        logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    log.addHandler(stream)
    log.setLevel(logging.DEBUG)

setup_test_log(log)


class FakeUnit(object):

    def __init__(self,
                 config,
                 remote_unit,
                 instance_metadata,
                 remote_members,
                 remote_data,
                 instance,
                 unit_name):

        self.config = config
        self.instance_metadata = instance_metadata
        self.remote_unit = remote_unit
        self.remote_data = remote_data
        self.remote_members = remote_members
        self.msgs = []
        self.instance = instance
        self.unit_name = unit_name

    @property
    def ec2metadata(self):
        return self.instance_metadata

    # If we can keep all the env var usage through unit, then
    # we can drop env vars modification in unit tests.
    @property
    def env_id(self):
        return os.environ.get('JUJU_ENV_UUID')

    @property
    def relation_id(self):
        return os.environ.get("JUJU_RELATION_ID")

    def get_instance(self, ec2):
        return self.instance

    def relation_get(self, unit_id=None):
        return self.remote_data.get(unit_id or self.remote_unit)

    def log(self, msg):
        self.msgs.append(msg)

    def config_get(self):
        return self.config


class Base(TestCase):

    region = "us-west-2"

    def update_environment(self, **kw):
        env = dict(os.environ)
        env.update(kw)
        self.change_environment(**env)

    def change_environment(self, **kw):
        """Reset the environment to kwargs. The tests runtime
        environment will be initialized with only those values passed
        as kwargs.

        The original state of the environment will be restored after
        the tests complete.
        """
        # preserve key elements needed for testing
        for k in os.environ:
            if k.startswith("AWS"):
                kw[k] = os.environ[k]

        original_environ = dict(os.environ)

        @self.addCleanup
        def cleanup_env():
            os.environ.clear()
            os.environ.update(original_environ)

        os.environ.clear()
        os.environ.update(kw)

    def get_config(self):
        return {'access-key-id': os.environ["AWS_ACCESS_KEY_ID"],
                'secret-access-key': os.environ["AWS_SECRET_ACCESS_KEY"]}

    def capture_logging(self, name="", level=logging.INFO,
                        log_file=None, formatter=None):
        if log_file is None:
            log_file = StringIO.StringIO()
        log_handler = logging.StreamHandler(log_file)
        if formatter:
            log_handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.addHandler(log_handler)
        old_logger_level = logger.level
        logger.setLevel(level)

        @self.addCleanup
        def reset_logging():
            logger.removeHandler(log_handler)
            logger.setLevel(old_logger_level)
        return log_file


class LockBase(Base):
    """Handle setting up dynamodb lock tables."""
    factory = None
    lock_table_name = None

    @classmethod
    def setUpClass(cls):
        cls.client = client = dynamodb.connect_to_region(cls.region)
        # Defer to controller lock name else test class lock name.
        table_name = cls.factory and cls.factory._lock_table_name or \
            cls.lock_table_name

        if table_name in client.list_tables():
            cls.table = client.get_table(table_name)
            return

        cls.table = client.create_table(
            table_name,
            client.create_schema(
                hash_key_name='key', hash_key_proto_value=str),
            20, 20)

        log.info("%s Waiting for lock table", cls.__name__)
        while True:
            if cls.table.status != 'ACTIVE':
                time.sleep(3)
                cls.table.refresh()
                continue
            break
        log.info("%s Lock table ready", cls.__name__)

    @classmethod
    def tearDownClass(cls):
        cls.client.delete_table(cls.table)


class EC2Base(LockBase):

    image_id = "ami-37d04607"  # Precise Current / Must match region
    instances = []
    groups = []

    @classmethod
    def setUpClass(cls):
        from boto import ec2
        cls.env_id = uuid.uuid4().hex
        cls.ec2 = ec2.connect_to_region(cls.region)
        cls.valid_zones = cls.ec2.get_all_zones()
        cls.start_instance(cls.valid_zones[0].name)
        cls.start_instance(cls.valid_zones[1].name)
        cls.wait_for_instances()
        super(EC2Base, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        cls.terminate_instances(cls.instances)
        super(EC2Base, cls).tearDownClass()

    @classmethod
    def wait_for_instances(cls):
        stime = time.time()
        log.info("%s setup, waiting for instances", cls.__name__)
        while True:
            try:
                result = cls.ec2.get_all_instances(cls.instances)
            except boto.exception.EC2ResponseError, e:
                if e.error_code == "InvalidInstanceId.NotFound":
                    time.sleep(4)
                    continue
                raise

            addresses = [r.instances[0].private_ip_address for r in result]
            addresses = filter(None, addresses)
            if len(addresses) == len(cls.instances):
                log.info(
                    "%s setup, instances started in %0.2fs",
                    cls.__name__, time.time() - stime)
                # Store the instances for tests
                cls.instances = [r.instances[0] for r in result]
                break
            time.sleep(4)

    @classmethod
    def terminate_instances(cls, instances):
        log.info(
            "%s teardown, terminating instances %s",
            cls.__name__, instances)
        try:
            cls.ec2.terminate_instances([i.id for i in instances])
        except Exception, e:
            log.info(
                "%s teardown, error instances %s", cls.__name__, e)

    @classmethod
    def start_instance(cls, zone):
        group_name = "test-%s-%d" % (cls.env_id[:10], len(cls.instances))
        cls.ec2.create_security_group(group_name, "test instance sec group")
        cls.groups.append(group_name)
        reservation = cls.ec2.run_instances(
            cls.image_id, placement=zone, security_groups=[group_name])
        # While we wait store for the instances store the instance ids
        cls.instances.append(reservation.instances[0].id)

    def get_unit(
            self, unit, data, members=(), rel_id="backend:1",
            instance=None, unit_name="aws/0"):
        self.change_environment(
            JUJU_ENV_UUID=self.env_id,
            JUJU_REMOTE_UNIT=unit,
            JUJU_RELATION_ID=rel_id)

        config = self.get_config()
        unit = FakeUnit(
            config,
            unit,
            {'availability-zone': self.region + 'a'},
            members,
            data,
            instance,
            unit_name)
        return unit
