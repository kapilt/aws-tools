import time
import logging

from boto.dynamodb.exceptions import (
    DynamoDBKeyNotFoundError,
    DynamoDBConditionalCheckFailedError)
from boto.dynamodb.item import Item

NotFound = DynamoDBKeyNotFoundError
CheckFailed = DynamoDBConditionalCheckFailedError

log = logging.getLogger("awsjuju.lock")


class LockAcquireError(Exception):
    """Couldn't obtain lock."""


class Lock(object):

    # Defaults for constructors, tests use separate values
    lock_table_name = "awsjuju-manage-locks"
    lock_table_options = {
        'hash': 'key',
        'throughput': (50, 10)}

    attempts = 3

    def __init__(self, client, client_id, table, key, ttl=60, delay=5,
                 attempts=None):
        self._client = client
        self._table = table
        self._key = key
        self._ttl = ttl
        self._client_id = client_id
        self._delay = delay
        self._locked = False

        if attempts is not None:
            self.attempts = attempts

    def acquire(self, key=None):
        attempts = self.attempts
        if key:
            self._key = key
        while attempts:
            self.gc()
            try:
                t = int(time.time())
                self._client.put_item(
                    Item(self._table, self._key,
                         attrs={"created": t,
                                "cid": self._client_id}),
                    dict(key=False))
                self._locked = t
                return self
            except CheckFailed:
                attempts -= 1
                if self._delay:
                    time.sleep(self._delay)
        log.info(
            "Client: %s could not acquire lock: %s",
            self._client_id, self._key)
        raise LockAcquireError(
            "Client: %s could not acquire lock on %s in %d attempts" % (
                self._client_id, self._key, self.attempts))

    def release(self):
        try:
            self._client.delete_item(
                Item(self._table, self._key),
                {'key': self._key, 'cid': self._client_id})
            self._locked = False
            return True
        except CheckFailed:
            if not self._locked:
                log.warning(
                    "Client: %s can't release unacquired lock: %s",
                    self._client_id, self._key)
            if self._locked:
                t = int(time.time()) - self._locked
                log.error(
                    "Client: %s lock: %s release fail, new owner. expired: %s",
                    self._client_id, self._key, t > 0)

    def gc(self):
        """ Opportunistic gc of stale locks """
        try:
            i = self._client.get_item(
                self._table, self._key, consistent_read=True)
        except NotFound:
            return True
        if i and (time.time() - i['created']) < self._ttl:
            return False
        log.debug("gc'ing stale lock on %s", self._key)
        try:
            self._client.delete_item(i, i)
            return True
        except NotFound:
            return True
        except CheckFailed:
            # Beaten to the punchline.
            return False

    def __enter__(self):
        if not self._locked:
            self.acquire()

    def __exit__(self, exc, value, tb):
        if isinstance(value, LockAcquireError):
            return
        self.release()
        self._locked = False
