
class BaseController(object):

    _ec2 = _dynamodb = _lock_table = _config = None
    _data_table = _table_name = _lock_table = _table_options = None

    # We share the lock table among all services.
    _lock_table_name = "awsjuju-manage-locks"
    _lock_table_options = {
        'hash': 'key',
        'throughput': (50, 10)}

    query_begins = BEGINS_WITH

    @classmethod
    def main(cls, op):
        try:
            method = getattr(cls(), "on_%s" % op)
            print "Invoking", method
            return method()
        except RetryLater:
            return

    def get_config(self):
        """Get the service configuration.
        """
        if self._config:
            return self._config

        data = self.unit.config_get()
        key = data['access-key-id']
        secret = data['secret-access-key']

        if not key or not secret:
            self.unit.log("No credentials set")
            raise InvalidConfig()

        self._config = data
        return data

    def get_credentials(self):
        """Get the provider iam credentials.
        """
        data = self.get_config()
        return dict(
            aws_access_key_id=data['access-key-id'],
            aws_secret_access_key=data['secret-access-key'])

    def get_ec2(self):
        """ Retrieve a connection to ec2.

        Access requirements depend on service in question.
        """
        if self._ec2:
            return self._ec2
        self._ec2 = ec2.connect_to_region(
            self.get_region(), **(self.get_credentials()))
        return self._ec2

    def get_region(self):
        """Get the region being operated on.
        """
        return self.unit.ec2metadata['availability-zone'][:-1]

    def get_lock(self, key, ttl=20, delay=10, attempts=3):
        """Obtain a resource lock on key for the specified duration/ttl.

        Lock acquisition will make up to arg: attempts number of tries
        to acquire the lock and will sleep for arg: delay seconds between
        attempts.
        """
        if not self._lock_table:
            self._lock_table = self._get_table(
                self._lock_table_name, self._lock_table_options)
        return Lock(
            self._dynamodb, self.unit.unit_name, self._lock_table,
            key, ttl, delay, attempts)

    def get_db(self):
        """Get the data table for the controller.
        """
        if not self._dynamodb:
            self._dynamodb = dynamodb.connect_to_region(
                self.get_region(), **(self.get_credentials()))

        if self._data_table is not None:
            return self._data_table

        if self._table_name is None or self._table_options is None:
            raise RuntimeError(
                "Db requested but table not specified %s",
                self.__class__.__name__)
        self._data_table = self._get_table(
            self._table_name, self._table_options)
        return self._data_table
