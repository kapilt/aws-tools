try:
    from yaml import CSafeLoader, CSafeDumper
except ImportError:
    optimized = False
else:
    optimized = True

from yaml import dump as _dump
from yaml import load as _load


def yaml_dump(value):
    if optimized:
        return _dump(value, Dumper=CSafeDumper, default_flow_style=False)
    return _dump(value, default_flow_style=False)


def yaml_load(value):
    if optimized:
        return _load(value, Loader=CSafeLoader)
    return _load(value)
