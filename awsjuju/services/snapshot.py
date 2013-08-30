"""
Backup/Snapshot policy management for AWS Instances.

Per Configuration, can take n daily, n weekly, n monthly snapshots.
"""

import argparse
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from dateutil.tz import tzutc
import logging
import os
import operator
import subprocess
import sys
import yaml

from awsjuju.common import get_or_create_table, BaseController
from awsjuju.unit import Unit
from awsjuju.lock import Lock

log = logging.getLogger("aws-snapshot")


INSTANCE_TABLE = "awsjuju-snapshot-instances"


class SnapshotRunner(object):

    # key to min time since last backup b4 we take a new one for the
    # period.

    allowed_periods = {
        "daily": timedelta(0.9),
        "weekly": timedelta(6.9),
        "monthly": timedelta(27.5)}


    def __init__(self, config, ec2, instance_db, lock):
        self.config = config
        self.ec2 = ec2
        self.instance_db = instance_db
        self.lock = lock

    def get_snapshot_instances(self, options):
        """ Get all instances registered for auto snapshots
        """

        if options.tag:
            return self._get_tagged_instances(options.tag)

        return self._get_registered_instances()

    def _get_tagged_instances(self, tag):
        """Support instance selection for backup based on a tag value.
        """
        tag_name, tag_value = tag.split(":", 1)
        for r in self.ec2.get_all_instances(
            filters={'tag:%s' % tag_name: tag_value}):
            for i in r.instances:
                yield ({}, i)

    def _get_registered_instances(self):
        """Support instance backup based on registration.
        """
        # hmm.. scan over all apps and all instances, sort of worst case.
        # this could be made more efficient, batching 20 instances at a time.
        for record in self.instance_db.scan():
            # TODO check for non existant instance and mark/dead
            results = self.ec2.get_all_instances([record['instance_id']])
            if not results:
                log.warning(
                    "Could not find registered instance %s",
                    record["instance_id"])
                continue
            for r in results:
                i = r.instances.pop()
                yield (record, i)

    def get_instance_volumes(self, i):
        if i.root_device_type != "ebs":
            log.warning(
                "Not backing up instance: %s non ebs root device", i.id)
            return
        devs = i.block_device_mapping.items()
        # Refuse the temptation to guess. If there are multiple volumes
        # attached to an instance, it could be raided/lvm/etc and we need
        # coordination with the instance to get a multi-volume consistent snap.
        if len(devs) > 2:
            log.warning(
                "Not backing up instance: %s, more than one volume", i.id)
            return

        for dev_name, bdt in devs:
            if not bdt.volume_id:
                continue
            yield bdt.volume_id, dev_name

    def run_period(self, options):
        """ Create backups for the given period for all registered instances.
        """

        period = options.period
        now = datetime.now(tzutc())
        log.info("Creating snapshots for %s on %s" % (
            period, now.strftime("%Y/%m/%d")))
        for r, i in self.get_snapshot_instances(options):
            with self.lock.acquire("snapshot-%s" % i.id):
                for vol_id, dev in self.get_instance_volumes(i):
                    self._snapshot_instance(r, i, vol_id, dev, now, period)

    def _snapshot_instance(self, r, i, vol_id, dev, now, period):
        """
        arg: r -> record
        arg: i -> boto ec2 instance
        arg: now -> datetime of cur time.
        """
        # Get previous snapshots
        snapshots = self.ec2.get_all_snapshots(
            filters={'tag:inst_snap': "%s/%s" % (i.id, period)})
        snapshots.sort(
            key=operator.attrgetter('start_time'), reverse=True)

        name = r.get('unit_name') or i.tags.get('Name') or i.id

        # Check if its too soon for a new snapshot from the last
        if snapshots:
            last_snapshot = date_parse(snapshots[0].start_time)
            if now - last_snapshot < self.allowed_periods[period]:
                log.warning(
                    "Skipping %s, last snapshot for %s was %s",
                    name, period, now - last_snapshot)
                return

        # Create new snapshot
        description = "%s %s %s" % (
            name, period.capitalize(), now.strftime("%Y-%m-%d"))
        log.debug("Snapshotting %s on %s as %s",
                  i.id, vol_id, description)
        snapshot = self.ec2.create_snapshot(vol_id, description)
        snapshot.add_tag('Name', description)
    
        # Copy over instance tags to the snapshot except name.
        for k, v in i.tags.items():
            if k == "Name":
                continue
            snapshot.add_tag(k, v)

        # If the instance was registered with an app id, and the
        # instance doesn't already have one, then copy over the
        # registed one as a tag.
        if 'app_id' in r and not 'app_id' in i.tags:
            snapshot.add_tag('app_id', r['app_id'])

        # Record metadata for restoration and backup system
        snapshot.add_tag('inst_snap', "%s/%s" % (i.id, period))
        snapshot.add_tag('inst_dev', dev)

        # Trim extras
        backup_count = self.config.get("%s-backups" % period)
        snapshots.insert(0, snapshot)
        if len(snapshots) <= backup_count:
            return
        log.info("Trimming excess %s snapshots %s" % (
            period,
            [s.tags.get('Name') for s in snapshots[backup_count:]]))

        for s in snapshots[backup_count:]:
            s.delete()

    def _get_instance(self, options):
        reservations = self.ec2.get_all_instances([options.instance_id])

        if not len(reservations) == 1:
            log.error("Invalid instance id %s" % options.instance_id)
            return
        if not len(reservations[0].instances) == 1:
            log.error("Invalid instance id %s" % options.instance_id)
            return
        instance = reservations[0].instances[0]
        return instance

    def register(self, options):
        """Register an instance for the snapshot system.
        """
        instance = getattr(options, 'instance', None)
        if instance is None:
            instance = self._get_instance(options)
            if instance is None:
                return
        log.info("Registering snapshot instance")

        vol_id = self.get_instance_volume(instance)
        if vol_id is None:
            return

        item = self.instance_db.new_item(
            options.app_id, instance.id, {
                'record': instance.id,
                'unit_name': options.unit and options.unit.strip() or ""})
        item.save()
        log.info("Instance %s registered for snapshots",
                 instance.id)
        return True


def setup_parser():
    parser = argparse.ArgumentParser("aws-snapshot")
    parser.add_argument(
        "-c", "--config", required=True, help="yaml config file")

    parser.add_argument(
        "-r", "--region", default="us-east-1",
        help="Region to operate in")

    subs = parser.add_subparsers()

    # Register instance
    sub_parser = subs.add_parser(
        "register", help="Register instances to the backup system")
    sub_parser.add_argument(
        "-i", "--instance", required=True, dest="instance_id")
    sub_parser.add_argument(
        "-a", "--app", required=True, dest="app_id")
    sub_parser.add_argument(
        "-u", "--unit")
    sub_parser.set_defaults(func='register')

    # Take snapshots for period.
    sub_parser = subs.add_parser(
        "run", help="Run the backup system")   
    sub_parser.add_argument(
        "-p", "--period", default="daily",
        choices=["daily", "weekly", "monthly"])
    sub_parser.add_argument(
        "-t", "--tag", default="",
        help="Backup instances matching tag, form is --tag=k:v")
    sub_parser.set_defaults(func='run_period')

    return parser


def cli():
    from boto import ec2
    from boto import dynamodb

    parser = setup_parser()
    options = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("boto").setLevel(logging.INFO)

    config_path = os.path.expandvars(os.path.expanduser(options.config))
    if config_path is None or not os.path.exists(config_path):
        raise ValueError("Invalid configuration path %r" % options.config)

    with open(config_path) as fh:
        config = yaml.load(fh.read())

    access_key = config.get('access-key', os.environ.get('AWS_ACCESS_KEY_ID'))
    secret_key = config.get('secret-key', os.environ.get('AWS_SECRET_ACCESS_KEY'))

    if not access_key or not secret_key:
        print access_key
        print secret_key
        print "AWS Keys must be specified in environment."
        sys.exit(1)
    
    ec2_api = ec2.connect_to_region(
        options.region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    db_api = dynamodb.connect_to_region(
        options.region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

    tagged_instances = options.tag or config.get("tag")
    if tagged_instances:
        pass

    log.debug("Setting up dynamodb tables")
    instance_db = get_or_create_table(
        db_api, INSTANCE_TABLE,
        {'hash': 'app_id', 'range': 'instance_id', 'throughput': (50, 10)})
    lock_db = get_or_create_table(
        db_api, Lock.lock_table_name, Lock.lock_table_options)
    lock = Lock(
        db_api, subprocess.check_output(['hostname']),
        lock_db, None, ttl=120, delay=10, attempts=3)

    log.debug("Starting snapshot runner")
    runner = SnapshotRunner(config, ec2_api, instance_db, lock)
    run_method = getattr(runner, options.func)
    run_method(options)


# Juju integration

class SnapshotController(BaseController):

    _table_name = "awsjuju-snapshots"
    _table_options = {}

    def __init__(self, unit=None):
        self.unit = unit or Unit()

    def on_joined(self):
        lock = self.get_lock("%s-%s" % (self.unit.env_id, self.unit.remote_unit))
        ec2 = self.get_config()
        instance = self.get_instance()


def main():
    import sys
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("boto").setLevel(logging.INFO)
    SnapshotController.main(sys.argv[1])

if __name__ == '__main__':
    main()
