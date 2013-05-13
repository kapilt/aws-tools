import json
import os
import inspect
import uuid
import unittest
import urllib2
import yaml

from rds import Controller
from test_common import EC2Base


if len(filter(None,
              (os.environ.get("AWS_SECRET_ACCESS_KEY"),
               os.environ.get("AWS_ACCESS_KEY_ID")))) != 2:
    raise ValueError("Required environment values not set")


class RDSTestCase(EC2Base):

    def setUp(self):
        from boto import rds
        self.env_id = uuid.uuid4().hex
        self.rds = rds.connect_to_region(self.region)
        config_path = os.path.join(
            os.path.dirname(inspect.getsourcefile(Controller)),
            "..",
            "config.yaml")
        self.config_defaults = self.load_config_defaults(config_path)
        self.group_rules = self.load_access_group_rules()

    def tearDown(self):
        for db in self.rds.get_all_dbinstances():
            if self.env_id in db.id:
                print "teardown", db.id
                # Will error out if db.status == "creating"
                self.rds.delete_dbinstance(
                    db.id,
                    skip_final_snapshot=True)
                # can't kill the security group yet.
                #self.rds.delete_dbsecurity_group(db.id)

    def load_access_group_rules(self):
        data = urllib2.urlopen("http://ifconfig.me/all.json").read()
        ip_data = json.loads(data)
        return [("%s/32" % (ip_data['ip_addr']), None, None)]

    def load_config_defaults(self, config_path):
        with open(config_path) as fh:
            charm_config = yaml.load(fh.read())
            charm_defaults = dict(
                filter(lambda (x, y): y is not None,
                       [(k, v.get('default')) for k, v in
                        charm_config['options'].items()]))
            return charm_defaults

    def get_config(self):
        config = super(RDSTestCase, self).get_config()
        config.update(self.config_defaults)
        config['allocated-storage'] = 5
        config['engine'] = 'mysql'
        config['engine-version'] = "5.5.27"
        config['instance-type'] = "db.m1.small"
        config['license-model'] = 'general-public-license'
        config['master-username'] = 'dbadmin'
        config['master-password'] = 'TeSTinG'
        return config

    def setup_unit_change(self, unit_name, address, change="joined"):
        unit = self.get_unit(
            unit_name,
            {unit_name: {'private-address': address}})
        controller = Controller(unit, self.group_rules)

        getattr(controller, "on_%s" % change)()
        return controller, unit

    def test_rds(self):
        try:
            self._test_rds()
        except:
           import pdb, traceback, sys
           traceback.print_exc()
           pdb.post_mortem(sys.exc_info()[-1])

    def _test_rds(self):
        controller_a, unit_a = self.setup_unit_change(
            'wordpress/0', self.addresses[0])
        db_id = controller_a.get_db_identifier()
        db = self.rds.get_all_dbinstances(db_id).pop()
        controller_b, unit_b = self.setup_unit_change(
            'wordpress/1', self.addresses[1])


if __name__ == '__main__':
    unittest.main()
