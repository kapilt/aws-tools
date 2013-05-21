from contextlib import contextmanager
import os
import random
import subprocess
import string
import sys
import time
import uuid

import boto.exception
from boto import rds
from common import KVFile, Unit

VOCAB = {
    'allocated-storage': {
        'mysql': (5, 1024),
        'oracle-se1': (10, 1024),
        'oracle-ee': (10, 1024),
        'sqlserver-ee': (200, 1024),
        'sqlserver-ex': (30, 1024),
        'sqlserver-web': (30, 1024)
        },
    'engine': [
        'MySQL5.1',
        ''],
    'instance-class': [
        'db.m1.small',
        'db.m1.large',
        'db.m1.xlarge',
        'db.m2.xlarge',
        'db.m2.2xlarge',
        'db.m2.4xlarge'
        ],
}


class Config(object):

    def __init__(self, config):
        self.config = config

    def get_parameters(self):
        config = self.config
        params = {}

        # Param translation
        for (source, target) in [
            ('allocated-storage', 'allocated_storage'),
            ('instance-type', 'instance_class'),
            ('engine', 'engine'),
            ('master-username', 'master_username'),
            ('master-password', 'master_password'),
            ('iops', 'provisioned-iops'),
            ('multi-az', 'multi_az'),
            ('engine-version', 'engine_version'),
            ('auto-minor-version-upgrade', 'auto_minor_version_upgrade'),
            ('license-mode', 'license_model')
            ]:
            if source in config:
                params[target] = config[source]
        return params

    def validate(self):
        return self

    def validate_window(self, window):
        pass

    def change_from(self, old_config):
        pass


class Controller(object):

    def __init__(self, unit=None, group_rules=None):
        state_path = os.path.join(os.environ.get("CHARM_DIR", ""), "rds.state")
        self._state = KVFile(state_path)
        self.unit = unit or Unit()
        self._group_rules = group_rules or ()

    def get_region(self):
        return self.unit.ec2metadata['availability-zone'][:-1]

    def get_rds(self, config):
        return rds.connect_to_region(self.get_region())

    def get_ec2(self, config):
        return rds.connect_to_region(self.get_region())

    def get_db(self, config, instance):
        if 'mysql' in config['engine'].lower():
            db = MySQL(config)
            if not self._state.get('mysql.client'):
                db.install_driver()
                self._state.set('mysql.client', True)
            return db

    def get_svc_dbname(self):
        return self.get_db_identifier()[:16].replace('-', '_')

    def get_db_identifier(self):
        service = os.environ["JUJU_REMOTE_UNIT"].split("/")[0]
        rel_id = os.environ["JUJU_RELATION_ID"].split(":")[-1]
        return "-".join([
            service,
            rel_id,
            os.environ["JUJU_ENV_UUID"]])

    def wait_for_db_instance(self, rds, instance_id):
        # Don't wait more than 10m
        print "test: waiting for db instance", instance_id,
        t = time.time()
        seen = set()
        while True:
            time.sleep(3)
            sys.stdout.write(".")
            try:
                dbs = rds.get_all_dbinstances(instance_id)
            except boto.exception.EC2ResponseError, e:
                if e.error_code == "InvalidInstanceId.NotFound":
                    continue
            db = dbs.pop()
            if db.status == "available":
                print "\ntest: db %s available in %s" % (
                    instance_id, time.time() - t)
                return db
            elif db.status not in seen:
                print "\n  db status", db.status, (time.time() - t),
                seen.add(db.status)
            if time.time() - t > 15 * 60:
                raise RuntimeError(
                    "Database not provisioned within threshold %s" % (
                        instance_id))

    def authorize_unit(self, rds):
        relation_id = self.get_db_identifier()
        group = rds.get_all_dbsecurity_groups(relation_id)
        group = group.pop()

        unit_instance = self.unit.get_instance()
        unit_group = [g.name for g in unit_instance.groups
                      if g.name[-1].isdigit()].pop()

        relation_db = self._state.get(relation_id)
        relation_db['service_units'] = {}
        relation_db['service_units'][os.environ['JUJU_REMOTE_UNIT']] = {
            'instance-id': unit_instance.id,
            'security-group': unit_group}
        self._state.set(relation_id, relation_db)
        group.authorize(ec2_group=unit_group)

    def deauthorize_unit(self, rds):
        relation_id = self.get_db_identifier()
        group = rds.get_all_dbsecurity_groups([relation_id])
        group = group.pop()

        unit_instance = self.unit.get_instance()
        unit_group = [g.name for g in unit_instance.groups
                      if g.name[-1].isdigit()].pop()

        relation_db = self._state.get(relation_id)
        remote_unit = os.environ['JUJU_REMOTE_UNIT']

        if remote_unit in relation_db.get('service_units', ()):
            del relation_db['service_units']
        group.revoke(ec2_group=unit_group)
        self._state.set(relation_id, relation_db)

    # Hooks
    def on_config_changed(self):
        config = Config(self.unit.config_get()).validate()

        errs = {}
        for k, v in self._state.get_all().items():
            errors = config.change_from(v)
            if errors is not None:
                errs[k] = v

        if errs:
            print "Configuration changes are not valid with existing state"
            print "Errors on the following relations"
            for k, v in errs.items():
                print "- relation k"
                for i in v:
                    print "  - %s" % v
            raise RuntimeError("Invalid configuration changes")

    def on_joined(self):
        relation_id = self.get_db_identifier()
        relation_db = self._state.get(relation_id)

        if relation_db is not None:
            self.authorize_unit()
            return

        config = Config(self.unit.config_get()).validate()
        rds = self.get_rds(config)

        # Create RDS Instance
        security_group = rds.create_dbsecurity_group(
            relation_id, relation_id)
        for rule in self._group_rules:
            cidr_ip, group_name, group_owner = rule
            rds.authorize_dbsecurity_group(
                security_group.name,
                cidr_ip,
                group_name,
                group_owner)
        params = config.get_parameters()
        params['id'] = self.get_db_identifier()
        params['security_groups'] = [security_group.name]
        instance = rds.create_dbinstance(**params)

        relation_db = dict(params)
        relation_db['instance-id'] = instance.id
        self._state.set(relation_id, relation_db)

        # Wait for db instance to be available
        instance = self.wait_for_db_instance(rds, instance.id)
        relation_db['endpoint'] = instance.endpoint
        self._state.set(relation_id, relation_db)

        # Initialize service database and principal
        db = self.get_db(relation_db, instance)
        db.connect()

        db_name = self.get_svc_dbname()
        relation_db['db_name'] = db_name
        db.create_database(db_name)
        user, password = db.create_service_user(db_name)
        relation_db['user'] = user
        relation_db['password'] = password
        self._state.set(relation_id, relation_db)

        self.authorize_unit(rds)
        self.unit.relation_set_multi({
            'host': instance.endpoint[0],
            'port': instance.endpoint[1],
            'database': db_name,
            'user': user,
            'password': password,
            'slave': False})

    def on_depart(self):
        self.deauthorize_unit()

    def on_broken(self):
        # Takes a final snapshot
        config = Config(self.unit.config_get()).validate()
        rds = self.get_rds(config)

        ident = self.get_db_identifier()
        db = rds.get_all_dbinstances(ident)
        if db is None:
            return
        rds.delete_dbinstance(db.id, final_snapshot_id="final-%s" % ident)

        # Probably doesn't work..
        rds.delete_dbsecurity_group(db.id)


class MySQL(object):

    def __init__(self, relation_db):
        self.relation_db = relation_db

    def install_driver(self):
        """Install database specific libraries."""
        subprocess.check_output([
            "sudo", "apt-get", "install", "-y", "python-mysqldb"])

    def connect(self):
        """
        """
        import MySQLdb
        host, port = self.relation_db['endpoint']
        self.conn = MySQLdb.connect(user=self.relation_db['master_username'],
                                    passwd=self.relation_db['master_password'],
                                    host=host,
                                    port=port)

    def create_database(self, name):
        """Create a database"""
        with CursorContext(self.conn) as cursor:
            cursor.execute("SHOW DATABASES")
            db_names = [i[0] for i in cursor.fetchall()]
            if not name in db_names:
                cursor.execute("CREATE DATABASE %s" % name)

    def create_user(self, db_name, user, password):
        """Create a database user"""
        with CursorContext(self.conn) as cursor:
            cursor.execute("grant all on `%s`.* to `%s` identified by '%s'" % (
                db_name,
                user,
                password))

    def create_service_user(self, db_name):
        user = "".join(random.sample(string.letters, 12))
        password = uuid.uuid4().hex
        self.create_user(db_name, user, password)
        return user, password


class Oracle(object):
    def install_driver(self):
        """
        http://lucasepe.blogspot.com/2010/05/installing-python-cxoracle-module-on.html
        http://maxolasersquad.blogspot.com/2011/04/cxoracle-on-ubuntu-1104-natty.html
        """


@contextmanager
def CursorContext(conn):
    try:
        cursor = conn.cursor()
        yield cursor
    except:
        cursor.close()
        raise


if __name__ == '__main__':
    import sys
    Controller.main(sys.argv[1])
