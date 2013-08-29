
import json
import os
import subprocess

from utils import yaml_load


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
        self._instance_cache = yaml_load(output)
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
