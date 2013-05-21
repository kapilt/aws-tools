import json
import os
import subprocess
import time
import yaml

from boto import ec2
from boto import dynamodb
from boto.dynamodb.condition import BEGINS_WITH
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

from awsjuju.lock import Lock


class RetryLater(Exception):
    pass


class InvalidConfig(Exception):
    pass

NotFound = DynamoDBKeyNotFoundError


class BaseController(object):

    _ec2 = _dynamodb = _lock_table = _config = None
    _data_table = _table_name = _lock_table = _table_options = None

    # We share the lock table among all services.
    _lock_table_name = "awsjuju-manage-locks"
    _lock_table_options = {
        'hash': 'key',
        'throughput': (50, 10)}

    query_begins = BEGINS_WITH

    @classmethod
    def main(cls, op):
        try:
            method = getattr(cls(), "on_%s" % op)
            print "Invoking", method
            return method()
        except RetryLater:
            return

    def get_config(self):
        """Get the service configuration.
        """
        if self._config:
            return self._config

        data = self.unit.config_get()
        key = data['access-key-id']
        secret = data['secret-access-key']

        if not key or not secret:
            self.unit.log("No credentials set")
            raise InvalidConfig()

        self._config = data
        return data

    def get_credentials(self):
        """Get the provider iam credentials.
        """
        data = self.get_config()
        return dict(
            aws_access_key_id=data['access-key-id'],
            aws_secret_access_key=data['secret-access-key'])

    def get_ec2(self):
        """ Retrieve a connection to ec2.

        Access requirements depend on service in question.
        """
        if self._ec2:
            return self._ec2
        self._ec2 = ec2.connect_to_region(
            self.get_region(), **(self.get_credentials()))
        return self._ec2

    def get_region(self):
        """Get the region being operated on.
        """
        return self.unit.ec2metadata['availability-zone'][:-1]

    def get_lock(self, key, ttl=20, delay=10, attempts=3):
        """Obtain a resource lock on key for the specified duration/ttl.

        Lock acquisition will make up to arg: attempts number of tries
        to acquire the lock and will sleep for arg: delay seconds between
        attempts.
        """
        if not self._lock_table:
            self._lock_table = self._get_table(
                self._lock_table_name, self._lock_table_options)
        return Lock(
            self._dynamodb, self.unit.unit_name, self._lock_table,
            key, ttl, delay, attempts)

    def get_db(self):
        """Get the data table for the controller.
        """
        if not self._dynamodb:
            self._dynamodb = dynamodb.connect_to_region(
                self.get_region(), **(self.get_credentials()))

        if self._data_table is not None:
            return self._data_table

        if self._table_name is None or self._table_options is None:
            raise RuntimeError(
                "Db requested but table not specified %s",
                self.__class__.__name__)
        self._data_table = self._get_table(
            self._table_name, self._table_options)
        return self._data_table


def get_or_create_table(dynamodb, name, options):
    """Get or create a table.
    """
    if name in dynamodb.list_tables():
        return dynamodb.get_table(name)

    params = [options['hash'], str]
    if options.get('range'):
        params.extend([options['range'], str])

    table = dynamodb.create_table(
        name,
        dynamodb.create_schema(*params),
        *options['throughput'])

    # Wait till the table is ready to use, about 15s
    while True:
        if table.status != 'ACTIVE':
            time.sleep(4)
            table.refresh()
            continue
        break
    return table


class Unit(object):

    def __init__(self):
        self._instance_cache = {}
        self._config_cache = {}
        self._relations_cache = {}

    def log(self, msg, level="info"):
        subprocess.check_call(["juju-log", msg])

    def config_get(self):
        if self._config_cache:
            return self._config_cache
        output = subprocess.check_output(["config-get", "--format", "json"])
        self._config_cache = json.loads(output)
        return self._config_cache

    def relation_ids(self, relation_name):
        args = ["relation-ids", "--format", "json", relation_name]
        output = subprocess.check_output(args)
        return json.loads(output)

    def relation_list(self, rel_id=None):
        args = ["relation-list", "--format", "json"]
        if rel_id:
            args.append("-r")
            args.append(rel_id)
        output = subprocess.check_output(args)
        return json.loads(output)

    def relation_get(self, unit_id=None):
        args = ["relation-get", "--format", "json"]
        if unit_id:
            args.append(unit_id)
        output = subprocess.check_output(args)
        return json.loads(output)

    def relation_set(self, key, value, rel_id=None):
        args = ["relation-set"]
        if rel_id:
            args.append("-r")
            args.append(rel_id)

        args.append("%s=%s" % (key, value))
        subprocess.check_output(args)

    def relation_set_multi(self, mapping, rel_id=None):
        args = ["relation-set"]
        if rel_id:
            args.append("-r")
            args.append(rel_id)

        for k, v in mapping.items():
            args.append("%s=%s" % (k, v))
        subprocess.check_output(args)

    def unit_get(self, key):
        args = ["unit-get", "--format", "json", key]
        output = subprocess.check_output(args)
        return json.loads(output)

    @property
    def unit_name(self):
        return os.environ["JUJU_UNIT_NAME"]

    @property
    def remote_unit(self):
        return os.environ['JUJU_REMOTE_UNIT']

    @property
    def env_uuid(self):
        return os.environ['JUJU_ENV_UUID']

    @property
    def relation_id(self):
        # KeyError if not in relation hook
        return os.environ["JUJU_RELATION_ID"]

    @property
    def ec2metadata(self):
        if self._instance_cache:
            return self._instance_cache
        output = subprocess.check_output(["ec2metadata"])
        self._instance_cache = yaml.load(output)
        return self._instance_cache

    def get_instance(self, ec2):
        """
        Get the remote instance id and zone the hook is currently
        executing for.

        Also saves the information for future use.
        """
        data = self.relation_get()
        address = data['private-address']

        if address.endswith('.internal'):
            filters = {'private-dns-name': address}
        else:
            filters = {'private-ip-address': address}
        result = ec2.get_all_instances(filters=filters)

        found = False
        for reservation in result:
            for instance in reservation.instances:
                if found is not False:
                    raise RuntimeError(
                        "Multiple instances found for unit %s %s" % (
                            self.unit.remote_unit,
                            " ".join([i.id for i in reservation.instances])))
                found = instance

        if found is not False:
            return found

        raise RuntimeError(
            "Couldn't find instance id for unit %s" % (
                self.remote_unit))

    # Common identifiers (service-rel-ident, service-ident, unit-ident)
    def get_service_identifier(self, size=32):
        # renders out to 'uuid-integer', max size is 32
        service = self.remote_unit.split("/")[0]
        idx = 32 - len(service)
        rel_id = self.relation_id.split(":")[-1]
        idx = idx - (len(rel_id) + 2)

        return "-".join([
            service,
            rel_id,
            self.env_uuid[:idx]])


class KVFile(object):

    def __init__(self, path):
        self.path = path

    def _load(self):
        if not os.path.exists(self.path):
            return {}

        with open(self.path) as fh:
            return json.load(fh)

    def get(self, key):
        return self._load().get(key)

    def get_all(self):
        return self._load()

    def set(self, key, value):
        data = self._load()

        with open(self.path, "w") as fh:
            data[key] = value
            json.dump(data, fh, indent=2)

    def remove(self, key):
        data = self._load()

        with open(self.path, "w") as fh:
            if key in data:
                del data[key]
            json.dump(data, fh, indent=2)
