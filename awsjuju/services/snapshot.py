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
import yaml

from awsjuju.common import get_or_create_table
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

    def get_snapshot_instances(self):
        """ Get all instances registered for auto snapshots
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
            return None
        devs = i.block_device_mapping.items()
        # Refuse the temptation to guess. If there are multiple volumes
        # attached to an instance, it could be raided/lvm/etc and we need
        # coordination with the instance to get a multi-volume consistent snap.
        if len(devs) > 2:
            log.warning(
                "Not backing up instance: %s, more than one volume", i.id)
            return None

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
        for r, i in self.get_snapshot_instances():
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
        snapshot.add_tag('app_id', r['app_id'])
        snapshot.add_tag('inst_snap', "%s/%s" % (i.id, period))
        snapshot.add_tag('dev', dev)

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

    def register(self, options):
        """Register an instance for the snapshot system.
        """
        reservations = self.ec2.get_all_instances([options.instance_id])

        if not len(reservations) == 1:
            log.error("Invalid instance id %s" % options.instance_id)
            return
        if not len(reservations[0].instances) == 1:
            log.error("Invalid instance id %s" % options.instance_id)
            return

        log.info("Registering snapshot instance")
        instance = reservations[0].instances[0]
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

    sub_parser = subs.add_parser(
        "register", help="Register instances to the backup system")
    sub_parser.add_argument(
        "-i", "--instance", required=True, dest="instance_id")
    sub_parser.add_argument(
        "-a", "--app", required=True, dest="app_id")
    sub_parser.add_argument(
        "-u", "--unit")
    sub_parser.set_defaults(func='register')

    sub_parser = subs.add_parser(
        "run", help="Run the backup system")
    sub_parser.add_argument(
        "-p", "--period", default="daily",
        choices=["daily", "weekly", "monthly"])
    sub_parser.set_defaults(func='run_period')

    return parser


def main():
    try:
        _main()
    except SystemExit:
        pass
    except:
        import pdb, traceback, sys
        traceback.print_exc()
        pdb.post_mortem(sys.exc_info()[-1])

def _main():
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
    secret_key = config.get('secret-key', os.environ.get('AWS_SECRET_KEY_ID'))

    ec2_api = ec2.connect_to_region(
        options.region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    db_api = dynamodb.connect_to_region(
        options.region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

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

if __name__ == '__main__':
    main()
