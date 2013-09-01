import sys
from awsjuju.common import Unit, BaseController


class Controller(BaseController):

    def __init__(self, unit=None):
        self.unit = unit or Unit()

    def get_tags(self):
        tags = {}
        config = self.unit.config_get()
        kv_pairs = config.get('tags').split(' ')
        for kv in kv_pairs:
            k, v = kv.split('=')
            tags[k.strip()] = v.strip()
        return tags

    def get_bootstrap(self, ec2):
        # TODO this seems to assume api stability around groups, should play nice
        # with either group. This also is our only ability at the moment to retrieve
        # the environment name.
        groups = [g.strip() for g in
                  self.ec2_metadata['security-groups'].split('\n')]
        groups.sort()
        env_group = groups.pop(0)
        env_group = "%s-0" % env_group
        result = ec2.get_all_instances(filters={
            'instance.group': env_group})
        return result[0].instances[0]

    def update_tags(self, instance, tags, only_unset=True):
        # be parsiminous as each tag modification is a roundtrip.
        updates = {}
        for k, v in tags.items():
            if only_unset and k in tags:
                continue
            if instance.tags.get(k) != v:
                updates[k] = v

        for k, v in updates.items():
            instance.add_tag(k, v)

    def on_joined(self):
        tags = self.get_tags()
        ec2 = self.get_ec2()

        instance = self.get_instance(ec2)
        tags['Name'] = self.unit.remote_unit
        tags['juju-env'] =  self.unit.env_uuid
        self.update_tags(instance, tags, only_unset=True)

        instance = self.get_bootstrap(ec2)
        tags['Name'] = 'juju-state-server-%s' % self.unit.env_uuid
        tags['juju-env'] =  self.unit.env_uuid        
        self.update_tags(instance, tags, only_unset=True)


def main():
    Controller.main(sys.argv[1])

if __name__ == '__main__':
    main()
