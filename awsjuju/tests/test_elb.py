import uuid
import time

from awsjuju.services.elb import Controller, RetryLater
from awsjuju.tests.common import EC2Base


class ELBTestCase(EC2Base):

    def setUp(self):
        self.env_id = uuid.uuid4().hex
        from boto.ec2 import elb
        self.elb = elb.connect_to_region(self.region)

    def tearDown(self):
        for b in self.get_balancers():
            print "tear down, destroy balancer", b.name
            self.elb.delete_load_balancer(b.name)

    def get_balancers(self):
        balancers = self.elb.get_all_load_balancers()
        for b in balancers:
            if self.env_id[:10] in b.name:
                yield b

    def test_create_load_balancer(self):
        unit = self.get_unit(
            'wordpress/1',
            {'wordpress/1': {
                'hostname': self.addresses[0],
                'port': 80}})
        controller = Controller(unit)
        controller.on_changed()
        balancers = list(self.get_balancers())
        self.assertEqual(len(balancers), 1)
        lb = balancers.pop()
        self.assertEqual(
            [self.instances[0]],
            [i.id for i in lb.instances])
        self.assertEqual(
            [self.valid_zones[0].name],
            [i for i in lb.availability_zones])
        self.assertEqual(lb.scheme, "internet-facing")
        self.assertEqual(
            str([(80, 80, 'HTTP')]),
            str(lb.listeners))
        self.assertEqual(lb.health_check.healthy_threshold, 10)
        self.assertEqual(lb.health_check.interval, 30)
        self.assertEqual(lb.health_check.target, 'TCP:80')
        self.assertEqual(lb.health_check.unhealthy_threshold, 2)
        self.assertEqual(lb.health_check.timeout, 5)

    def test_multiple_units_multiple_zone(self):
        unit = self.get_unit(
            'wordpress/0',
            {'wordpress/0': {
                'hostname': self.addresses[0],
                'port': 80}})
        controller = Controller(unit)
        controller.on_changed()

        unit = self.get_unit(
            'wordpress/1',
            {'wordpress/1': {
                'hostname': self.addresses[1],
                'port': 80}})
        controller = Controller(unit)
        controller.on_changed()

        # Check for multi-zone
        balancers = list(self.get_balancers())
        self.assertEqual(len(balancers), 1)
        lb = balancers.pop()
        self.assertEqual(
            sorted([self.valid_zones[0].name, self.valid_zones[1].name]),
            sorted([i for i in lb.availability_zones]))
        self.assertEqual(
            sorted(self.instances),
            sorted([i.id for i in lb.instances]))

        # Check zone reduction
        controller.on_depart()
        balancers = list(self.get_balancers())
        self.assertEqual(len(balancers), 1)
        lb = balancers.pop()
        self.assertEqual(
            [self.valid_zones[0].name],
            [i for i in lb.availability_zones])
        self.assertEqual(
            [self.instances[0]],
            [i.id for i in lb.instances])

    def test_lb_destroy(self):
        unit = self.get_unit(
            'wordpress/1',
            {'wordpress/1': {
                'hostname': self.addresses[0],
                'port': 80}})
        controller = Controller(unit)
        controller.on_changed()
        controller.on_broken()
        time.sleep(2)
        balancers = list(self.get_balancers())
        self.assertEqual(len(balancers), 0)

    def test_unit_not_ready(self):
        unit = self.get_unit(
            'wordpress/1',
            {'wordpress/1': {
                'private-address': self.addresses[0]}})
        controller = Controller(unit)
        self.assertRaises(RetryLater, controller.on_changed)


if __name__ == '__main__':
    import unittest2
    unittest2.main()
