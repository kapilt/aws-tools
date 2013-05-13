from awsjuju.lock import Lock, LockAcquireError
from awsjuju.tests.common import LockBase


class LockTest(LockBase):

    _lock_table_name = "awsjuju-lock-test"

    def setUp(self):
        self.output = self.capture_logging("awsjuju.lock")

    def get_lock(self, key, cid, ttl=5, delay=0):
        return Lock(self.client, cid, self.table, key, ttl, delay=delay)

    def test_lock(self):
        lock_a = self.get_lock("wasdc", 'mayor')
        lock_b = self.get_lock("wasdc", 'council')

        self.assertTrue(lock_a.acquire())
        self.assertRaises(LockAcquireError, lock_b.acquire)
        self.assertIn(
            "Client: council could not acquire lock: wasdc",
            self.output.getvalue())
        self.assertTrue(lock_a.release())
        self.assertFalse(lock_a.release())
        self.assertIn(
            "Client: mayor can't release unacquired lock: wasdc",
            self.output.getvalue())
        self.assertTrue(lock_b.acquire())
        self.assertTrue(lock_b.release())

    def test_lock_gc(self):
        lock_a = self.get_lock("sf", 'mayor', ttl=1)
        lock_b = self.get_lock("sf", 'council', ttl=1, delay=1)
        self.assertTrue(lock_a.acquire())
        self.assertTrue(lock_b.acquire())
        self.assertFalse(lock_a.release())
        self.assertIn(
            "Client: mayor lock: sf release fail, new owner. expired: True",
            self.output.getvalue())

if __name__ == '__main__':
    import unittest
    unittest.main()
