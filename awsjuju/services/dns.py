import os

from awsjuju.common import Unit, BaseController, InvalidConfig


class Controller(BaseController):

    _table_name = "awsjuju-route53"
    _table_options = {
        'hash': 'app_id',
        'range': 'rel_unit',
        'throughput': (50, 10)}  # (read, write, 10usd month)

    def __init__(self, unit=None):
        self.unit = unit or Unit()

    def get_dns(self, config):
        # Lazy load, route53 has some questionable reliance on env variables
        os.environ["AWS_SECRET_ACCCES_KEY"] = config['secret-access-key']
        os.environ["AWS_ACCESS_KEY_ID"] = config['access-key-id']

        from area53 import route53
        dns = route53.get_zone(config['zone'])
        if dns is None:
            raise InvalidConfig(
                "Invalid zone %s, use domain name" % config['zone'])
        return dns

    def get_instance_address(self):
        """Get the public ip address for a remote unit.
        """
        ec2 = self.get_ec2()
        instance = self.unit.get_instance(ec2)
        return instance.id, instance.ip_address

    def get_hostname(self, config):
        remote_unit = self.unit.remote_unit.replace("/", "-")
        host_name = "%s.%s" % (remote_unit, config['zone'])
        if config.get('prefix'):
            host_name = "%s%s" % (config['prefix'], host_name)
        return host_name

    # Hook methods
    def on_joined(self):
        config = self.unit.config_get()
        dns = self.get_dns(config)
        db = self.get_db()

        with self.get_lock("%s-%s" % (
                self.unit.env_id, self.unit.relation_id)):
            host_name = self.get_hostname(config)
            instance_id, ip_address = self.get_instance_address()
            record = db.new_item(
                self.unit.env_id, "%s-%s" % (
                    self.unit.relation_id, self.unit.remote_unit),
                {'instance_id': instance_id, 'addr': ip_address,
                 'host': host_name, 'zone': config['zone']})
            entry = dns.get_a(host_name)
            if entry is not None:
                dns.update_a(host_name, ip_address, config['ttl'])
            else:
                dns.add_a(host_name, ip_address, config['ttl'])
            record.put()

    def on_depart(self):
        config = self.unit.config_get()
        dns = self.get_dns(config)
        db = self.get_db()

        with self.get_lock(
                "%s-%s" % (self.unit.env_id, self.unit.relation_id)):
            record = db.get_item(
                self.unit.env_id, "%s-%s" % (
                    self.unit.relation_id, self.unit.remote_unit))
            host_name = self.get_hostname(config)
            dns.delete_a(host_name)
            record.delete()

    def on_broken(self):
        config = self.unit.config_get()
        dns = self.get_dns(config)
        db = self.get_db()

        with self.get_lock("%s-%s" % (
                self.unit.env_id, self.unit.relation_id)):
            result = db.query(
                self.unit.env_id,
                range_key_condition=self.query_begins(self.unit.relation_id))
            for record in result:
                dns.delete_a(record['host'])

if __name__ == '__main__':
    import sys
    Controller.main(sys.argv[1])
