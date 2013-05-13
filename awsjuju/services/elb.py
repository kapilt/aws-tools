import os
import boto
import boto.ec2.elb as elb

# Monkey patch a fix onto boto
from boto.ec2.elb.loadbalancer import LoadBalancerZones
from common import KVFile, Unit, BaseController, RetryLater

if not getattr(LoadBalancerZones, 'endElement', None):
    def endElement(self, name, value, connection):
        pass
    setattr(LoadBalancerZones, 'endElement', endElement)


class Controller(BaseController):

    def __init__(self, unit=None):
        self.unit = unit or Unit()
        self._elb = None
        self._ec2 = None
        self._config = None
        state_path = os.path.join(os.environ.get("CHARM_DIR", ""), "elb.state")
        self._state = KVFile(state_path)

    def get_zones(self):
        """
        Get all the zones currently in use by backend instances
        """
        zones = []
        for k, v in self._state.get_all().items():
            zones.append(v['zone'])
        return zones

    def get_elb(self):
        """
        Get the charm elb abstraction.
        """
        if self._elb:
            return self._elb

        data = self.get_config()
        self._elb = ELB(
            data['access-key-id'],
            data['secret-access-key'],
            self.get_region(),
            self.unit.get_service_identifier())

        return self._elb

    def get_instance(self):
        """
        Get the remote instance id and zone the hook is currently
        executing for.

        Also saves the information for future use.
        """
        data = self.unit.relation_get()
        port = data.get('port')
        address = data.get('hostname')

        if not port or not address:
            self.unit.log('Peer address not set, waiting for handshake')
            raise RetryLater()

        ec2 = self.get_ec2()
        instance = self.unit.get_instance(ec2)
        return instance.id, instance.placement

    def on_changed(self):
        """Called when a unit changes it settings or comes online.
        """
        lb = self.get_elb()
        instance_id, zone = self.get_instance()
        if instance_id is None:
            return

        if lb.exists():
            print "added %s to elb %s" % (instance_id, lb.elb_name)
            lb.add(instance_id, zone)
        else:
            print "creating elb %s" % (lb.elb_name)
            lb.create([zone])
            lb.add(instance_id, zone)

    def on_depart(self):
        """Called when a unit is no longer available.
        """
        data = self._state.get(self.unit.remote_unit)
        if data is None:
            print "could not find remote unit %s" % self.unit.remote_unit
            return
        instance_id = data['instance-id']
        lb = self.get_elb()
        if lb.exists():
            print "removed %s from elb %s" % (instance_id, lb.elb_name)
            lb.remove(instance_id)
            self._state.remove(self.unit.remote_unit)
            lb.sync(self.get_zones())

    def on_broken(self):
        """Called when the relationship is broken.
        """
        lb = self.get_elb()
        if lb.exists():
            print "removed elb %s" % (lb.elb_name)
            lb.destroy()

    def on_config_changed(self):
        lb = self.get_elb()
        print lb


class ELB(object):

    def __init__(self, key, secret, region, elb_name):
        self.elb = elb.connect_to_region(
            region,
            aws_access_key_id=key,
            aws_secret_access_key=secret)
        self.elb_name = elb_name
        self._boto_lb = None

    def exists(self):
        try:
            result = self.elb.get_all_load_balancers([self.elb_name])
        except boto.exception.BotoServerError, e:
            if e.error_code != "LoadBalancerNotFound":
                raise
            return False
        self._boto_lb = result.pop()
        return True

    def create(self, zones):
        return self.elb.create_load_balancer(
            name=self.elb_name,
            zones=zones,
            listeners=[(80, 80, 'HTTP')])

    def add(self, instance_id, zone):
        self.elb.enable_availability_zones(self.elb_name, [zone])
        self.elb.register_instances(self.elb_name, [instance_id])

    def remove(self, instance_id, zone=None):
        self.elb.deregister_instances(self.elb_name, [instance_id])

    def destroy(self):
        self.elb.delete_load_balancer(self.elb_name)

    def sync(self, zones):
        if not self._boto_lb:
            raise KeyError("Invalid usage, must be called after exists()")

        existing = set(self._boto_lb.availability_zones)
        current = set(zones)
        new = current - existing
        old = existing - current

        if old:
            self.elb.disable_availability_zones(self.elb_name, list(old))
        if new:
            self.elb.enable_availability_zones(self.elb_name, list(new))
        print "removed zones", list(old)
        return new, old


if __name__ == '__main__':
    import sys
    Controller.main(sys.argv[1])
