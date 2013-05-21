from awsjuju.common import Unit, BaseController


class Controller(BaseController):

    def __init__(self, unit=None):
        self.unit = unit or Unit()

    def on_joined(self):
        tags = self.get_tags()
        ec2 = self.get_ec2()

        instance = self.get_instance(ec2)
        tags['Name'] = self.unit.remote_unit
        self.update_tags(instance, tags)

        instance = self.get_bootstrap(ec2)
        tags['Name'] = 'juju-state-server'
        self.update_tags(instance, tags)

    def get_tags(self):
        tags = {}
        config = self.unit.config_get()
        kv_pairs = config.get('tags').split(' ')
        for kv in kv_pairs:
            k, v = kv.split('=')
            tags[k.strip()] = v.strip()
        return tags

    def get_bootstrap(self, ec2):
        groups = [g.strip() for g in
                  self.ec2_metadata['security-groups'].split('\n')]
        groups.sort()
        env_group = groups.pop(0)
        env_group = "%s-0" % env_group
        result = ec2.get_all_instances(filters={
            'instance.group': env_group})
        return result[0].instances[0]

    def update_tags(self, instance, tags):
        # be parsiminous as each tag is a roundtrip.
        updates = {}
        for k, v in tags.items():
            if instance.tags.get(k) != v:
                updates[k] = v

        for k, v in updates.items():
            instance.add_tag(k, v)


def main():
    pass


if __name__ == '__main__':
    main()
