"""
Backup/Snapshot policy management for AWS Instances.

Per Configuration, can take n daily, n weekly, n monthly snapshots.
"""

import argparse
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
from dateutil.tz import tzutc
#import json
import logging
import os
import operator
import subprocess
import sys
import time
import yaml

from awsjuju.common import get_or_create_table, BaseController
from awsjuju.unit import Unit
from awsjuju.lock import Lock

log = logging.getLogger("aws-snapshot")


INSTANCE_TABLE = "awsjuju-snapshot-instances"


class SnapshotBase(object):

    allowed_periods = {
        "daily": timedelta(0.9),
        "weekly": timedelta(6.9),
        "monthly": timedelta(27.5)}


    def __init__(self, config, ec2, instance_db, lock):
        self.config = config
        self.ec2 = ec2
        self.instance_db = instance_db
        self.lock = lock

    @staticmethod
    def _flatten_instances(results):
        instances = []
        for reservation in results:
            instances.extend(reservation.instances)
        return instances

    def _get_tag_query(self, tags):
        """Form taggable resource query for a given set of tags."""
        q = {}
        for t in tags:
            tag_name, tag_value = t.split(":", 1)
            q.update({'tag:%s' % tag_name: tag_value})
        return q

    def _get_tagged_instances(self, tags=(), groups=(), instances=()):
        """Support instance selection for backup based on a criteria value.
        """
        q = {}
        tags = tags or ()
        groups = groups or ()
        instances = instances or ()

        q.update(self._get_tag_query(tags))

        for g in groups:
            if g.startswith('sg'):
                q.update({'group-id': g})
            else:
                q.update({'group-name': g})

        log.debug("Querying instances: %s filters: %s" % (
            instances, q))

        instances = []
        for r in self.ec2.get_all_instances(instance_ids=instances, filters=q):
            for i in r.instances:
                instances.append(({}, i))
        return instances

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
                    "Could not fid registered instance %s",
                    record["instance_id"])
                continue
            for r in results:
                i = r.instances.pop()
                yield (record, i)

    def _get_instance_snapshots(
        self, instances=None, period=None, groups=None, tags=()):
        """  """
        query = {}
        if instances and period:
            for i in instances:
                query.update({'tag:inst_snap': "%s/%s" % (i, period)})
                break

        if (not instances and not groups) and tags:
            query.update(self._get_tag_query(tags))

        # Get previous snapshots
        log.debug("Querying snapshots with filters: %s" % (query))
        snapshots = self.ec2.get_all_snapshots(
            owner="self",
            filters=query)
        snapshots.sort(
            key=operator.attrgetter('start_time'), reverse=True)
        log.debug("Found %d instance device snapshots" % (len(snapshots)))
        return snapshots

    def get_snapshot_instances(self, options):
        """ Get instances registered/queried for snapshots
        """
        if options.tags or options.groups:
            return self._get_tagged_instances(options.tags, options.groups)

        return self._get_registered_instances()


class SnapshotInstances(SnapshotBase):

    def run(self, options):
        """ Create backups for the given period for all registered instances.
        """
        period = options.period
        now = datetime.now(tzutc())
        log.info("Creating snapshots for %s on %s" % (
            period, now.strftime("%Y/%m/%d")))

        #import pdb; pdb.set_trace()
        for r, i in self.get_snapshot_instances(options):
            log.info("Processing instance %s:%s" % (i.id, i.tags["Name"]))
            with self.lock.acquire("snapshot-%s" % i.id):
                snapshots = self._get_instance_snapshots(
                    instances=[i.id], period=period)
                for vol_id, dev in self.get_instance_volumes(i):
                    device_snapshots = [
                        s for s in snapshots if s.tags.get('inst_dev') == dev]

                    self._snapshot_instance(
                        r, i, vol_id, dev, now, period, device_snapshots)

    def get_instance_volumes(self, i):
        if i.root_device_type != "ebs":
            log.warning(
                "Not backing up instance: %s/%s non ebs root device", i.id, i.tags.get("Name", "NA"))
            return
        devs = i.block_device_mapping.items()
        # Refuse the temptation to guess. If there are multiple volumes
        # attached to an instance, it could be raided/lvm/etc and we need
        # coordination with the instance to get a multi-volume consistent snap.
        if len(devs) > 2:
            log.warning(
                "Not backing up instance: %s/%s, more than one volume", i.id, i.tags.get("Name", "NA"))
            return

        for dev_name, bdt in devs:
            if not bdt.volume_id:
                continue
            yield bdt.volume_id, dev_name

    def _snapshot_instance(self, r, i, vol_id, dev, now, period, snapshots):
        """
        arg: r -> record
        arg: i -> boto ec2 instance
        arg: now -> datetime of cur time.
        """
        name = r.get('unit_name') or i.tags.get('Name') or i.id

        # Check if its too soon for a new snapshot from the last
        if snapshots:
            last_snapshot = date_parse(snapshots[0].start_time)
            if now - last_snapshot < self.allowed_periods[period]:
                log.warning(
                    "Skipping %s, last snapshot for %s was %s" % (
                    name, period, now - last_snapshot))
                return

        # Create new snapshot
        description = "%s %s %s" % (
            name, period.capitalize(), now.strftime("%Y-%m-%d"))
        log.debug("Snapshotting %s on %s as %s" % (
                  i.id, vol_id, description))
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
        log.info("Trimming excess %s snapshots %s max:%d existing:%d" % (
            period,
            [s.tags.get('Name') for s in snapshots[backup_count:]],
            backup_count,
            len(snapshots)))

        for s in snapshots[backup_count:]:
            s.delete()


class ListSnapshots(SnapshotBase):

    def run(self, options):
        instances = filter(None, options.instances)

        if not instances and options.groups:
            groups = filter(None, options.groups)
            if groups:
                instances = self._get_tagged_instances(
                    groups=groups,
                    tags=options.tags)

        snapshots = self._get_instance_snapshots(
            instances=filter(None, options.instances), 
            tags=options.tags,
            period="daily")

        results = []
        for s in snapshots:
            if not s.progress == "100%":
                continue
            results.append(
                {'name': s.tags.get('Name'), 
                 'created': s.start_time,
                 'volume': s.volume_id,
                 'instance': s.tags})
        import json
        print json.dumps(results, indent=2)
        return snapshots


class RestoreSnapshots(SnapshotBase):

    def run(self, options):

        r_date = date_parse(options.date)

        # Retrieve all the instances to restore.
        instances = self._get_tagged_instances(
            instances=options.instances, 
            groups=options.groups, 
            tags=options.tags)
        log.info("Found %d instances to restore" % len(instances))

        # Find the appropriate subset of snapshots to utilize or error.
        log.info("Finding candidate backups for %s", options.date)
        snapshots = self._get_instance_snapshots(
            instances=instances, 
            tags=options.tags, 
            period=options.period)

        instance_snapshots = self._get_snapshots_by_instance_for_time(
            snapshots, r_date, options.max_skew_seconds)

        # Volume creation is concurrent but long running.
        log.info("Creating volumes to restore")        
        instance_volume_map = self._create_snapshot_volumes(instance_snapshots)

        log.info("Stopping instances to restore")
        self._stop_instances(instances)

        log.info("Attaching restored volumes")
        self._attach_volumes(instance_volume_map)

        log.info("Starting instances")        
        self.ec2.start_instances(instances)

    def _get_snapshots_by_instance_for_time(self, snapshots, r_date, skew):
        instance_snapshots = {}

        for s in snapshots:
            start_date = date_parse(s.start_time)
            if start_date < r_date:
                continue

            inst_id, period = s.tags.get("inst_snap", "/").split("/", 1)
            if not inst_id:
                log.warning(
                    "Snapshot with invalid inst_snapshot %s %s" % (
                        s.id, s.tags))
            if inst_id not in instance_snapshots:
                instance_snapshots[inst_id] = s
                continue

            if date_parse(instance_snapshots[inst_id].start_time) > start_date:
                instance_snapshots[inst_id] = s

        return instance_snapshots

    def _attach_volumes(
        self, instance_map, instance_volume_map, instance_snapshot_map):
        """Attach volumes to instances
        """
        for instance_id, vol_id in instance_volume_map.items():
            volume = instance_volume_map[instance_id]
            snapshot = instance_snapshot_map[instance_id]
            device = snapshot.tags['inst_dev']
            instance = instance_map[instance_id]

            # Detach the old one if attached
            if device in instance.block_device_mapping:
                previous_device = instance.block_device_mapping[device]
                # Cant if previous device is the restored volume since it varies
                # for restore multiple times from pitr. 
                self.ec2.detach_volume(instance_id, previous_device.volume_id)

            # Attach the new one
            self.ec2.attach_volume(instance_id, volume.id)

    def _stop_instances(self, instances):
        stopping_instances = [i.id for i in instances if i.state == "running"]
        self.ec2.stop_instances(stopping_instances)

        while stopping_instances:
            log.info("Waiting for instances to stop: %s" % stopping_instances)
            stopping = self._flatten_instances(
                self.ec2.get_all_instances(instance_ids=stopping_instances))
            stopping_instances =  [i.id for i in stopping if i.state in (
                'running', 'stopping')]
            time.sleep(10)

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


class RegisterInstance(SnapshotBase):

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


class RemoveSnapshots(SnapshotBase):

    def run(self, options):
        # TODO can't query for dead/terminated instances, we could just be 
        # doing garbage collection.
        #instances = self.get_snapshot_instances(options)
        #log.info("Removing snapshots for these instances %d" % len(instances))

        snapshots = self._get_instance_snapshots(
            tags=options.tags,
            instances=filter(None, options.instances),
            period=options.period)

        log.info("Removing %d snapshots for period %s" % (
            len(snapshots), options.period))

        for s in snapshots:
            self.ec2.delete_snapshot(s.id)


def setup_parser():
    parser = argparse.ArgumentParser("aws-snapshot")
    parser.add_argument(
        "-c", "--config", required=True, help="yaml config file")

    parser.add_argument(
        "-r", "--region", default="us-east-1",
        help="Region to operate in")

    def add_query_options(subparser):
        sub_parser.add_argument(
            "-t", "--tag", dest="tags", action="append",
            help="Backup instances matching tag, form is --tag=k:v")
        sub_parser.add_argument(
            "-s", "--group", dest="groups", action="append",
            help="Backup instances matching security group name or id")    
        sub_parser.add_argument("instances", nargs="?", action="append")

    subs = parser.add_subparsers()

    sub_parser = subs.add_parser(
        "register", 
        help="Register instances to the backup system")
    sub_parser.add_argument(
        "-i", "--instance", required=True, dest="instance_id")
    sub_parser.add_argument(
        "-a", "--app", required=True, dest="app_id")
    sub_parser.add_argument(
        "-u", "--unit")
    sub_parser.set_defaults(cmd=RegisterInstance)


    sub_parser = subs.add_parser(
        "run", 
        help="Run the backup system")   
    sub_parser.set_defaults(cmd=SnapshotInstances)
    sub_parser.add_argument(
        "-p", "--period", default="daily",
        choices=["daily", "weekly", "monthly"])
    add_query_options(sub_parser)


    sub_parser = subs.add_parser(
        "remove", 
        help="Run the backup system")   
    sub_parser.set_defaults(cmd=RemoveSnapshots)
    sub_parser.add_argument(
        "-p", "--period", default="daily",
        choices=["daily", "weekly", "monthly"])
    add_query_options(sub_parser)


    sub_parser = subs.add_parser(
        "list", 
        help="List backups for an instance")
    sub_parser.set_defaults(cmd=ListSnapshots)
    add_query_options(sub_parser)


    sub_parser = subs.add_parser(
        "restore", 
        help="Restore instances from backups")
    sub_parser.set_defaults(cmd=RestoreSnapshots)
    sub_parser.add_argument(
        "-d", "--date",
        help="Use snapshot from before this date")
    sub_parser.add_argument(
        "-p", "--period", default=None,
        help="Only use this type of backup (weekly, daily, month) default all")
    add_query_options(sub_parser)

    return parser


def cli():
    from boto import ec2
    from boto import dynamodb

    parser = setup_parser()
    options = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(msg)s")
    logging.getLogger("boto").setLevel(logging.INFO)

    config_path = os.path.expandvars(os.path.expanduser(options.config))
    if config_path is None or not os.path.exists(config_path):
        raise ValueError("Invalid configuration path %r" % options.config)

    with open(config_path) as fh:
        config = yaml.load(fh.read())

    access_key = config.get('access-key', os.environ.get('AWS_ACCESS_KEY_ID'))
    secret_key = config.get('secret-key', os.environ.get('AWS_SECRET_ACCESS_KEY'))

    if not access_key or not secret_key:
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

    tagged_instances = options.tags or config.get("tags")
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

    log.debug("Running cmd: %s" % options.cmd.__name__)
    command = options.cmd(config, ec2_api, instance_db, lock)
    try:
        command.run(options)
    except:
        import pdb, sys, traceback
        traceback.print_exc()
        pdb.post_mortem(sys.exc_info()[-1])

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
