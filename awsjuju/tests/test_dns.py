import os
import unittest2

from awsjuju.services.dns import Controller
from awsjuju.tests.common import EC2Base


@unittest2.skipIf(
    (os.environ.get("AWS_SECRET_KEY_ID") and
     os.environ.get("AWS_ACCESS_KEY_ID") and
     os.environ.get("AWS_ROUTE53_ZONE")),
    "Route53 Tests: required environment values not set")
class DNSTestCase(EC2Base):

    factory = Controller

    def setUp(self):
        from area53 import route53
        self.dns = route53.get_zone(os.environ['AWS_ROUTE53_ZONE'])

    def tearDown(self):
        unit = self.get_unit("foi", None)
        db = Controller(unit).get_db()
        db.delete()

    def get_config(self):
        config = super(DNSTestCase, self).get_config()
        config['zone'] = self.zone
        config['prefix'] = 'test-'
        config['ttl'] = 10
        return config

    @property
    def zone(self):
        return os.environ["AWS_ROUTE53_ZONE"]

    def setup_unit_change(self, unit_name, instance, change="joined"):

        if instance is None:
            unit_data = {}
        else:
            unit_data = {'private-address': instance.private_ip_address}

        unit = self.get_unit(
            unit_name,
            {unit_name: unit_data},
            instance=instance)
        controller = Controller(unit)
        getattr(controller, "on_%s" % change)()
        config = unit.config_get()
        return controller.get_hostname(config)

    def test_dns(self):
        host_a = self.setup_unit_change(
            'wordpress/0', self.instances[0])
        host_b = self.setup_unit_change(
            'wordpress/1', self.instances[1])

        record = self.dns.get_a(host_a)
        self.assertEqual(
            record.name, "test-wordpress-0.%s." % self.zone)
        self.assertEqual(record.type, "A")
        self.assertIn(self.instances[0].ip_address, record.resource_records)

        record = self.dns.get_a(host_b)
        self.assertEqual(
            record.name, "test-wordpress-1.%s." % self.zone)
        self.assertEqual(record.type, "A")
        self.assertIn(self.instances[1].ip_address, record.resource_records)

        self.setup_unit_change(
            'wordpress/1',
            self.instances[1], 'depart')
        record = self.dns.get_a(host_b)
        self.assertEqual(record, None)

        self.setup_unit_change('', None, 'broken')

        # Just the two records for the zone itself.
        records = self.dns.get_records()
        self.assertEqual(len(records), 2)

if __name__ == '__main__':
    import unittest2
    unittest2.main()
