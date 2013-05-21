# Not Used.. slowly being migrated.
from dynamodb_mapper.model import DynamoDBModel


class RDS(DynamoDBModel):
    __table__ = "manage-rds"
    __hash_key__ = "app_id"
    __range_key__ = "service_stamp"

    _schema = {
        "app_id": str,
        "service_stamp": str,
        "rds_id": str,
    }


class ELB(DynamoDBModel):
    __table__ = "manage-elbs"
    __hash_key__ = "app_id"
    __range_key__ = "rel_id"

    _schema = {
        "app_id": str,
        "rel_id": str,
        "elb_id": str,
        "instance_id": str,
        "zone": str,
    }
