# -*- coding: utf-8 -*-
"""
    Elasticache Client
    @author mindosilalahi@livenation.com
"""
from base64 import b64encode, b64decode
import gzip
import io
import json
import os

from cachetools.func import ttl_cache
import elasticache_auto_discovery
from pymemcache.client.hash import HashClient
from pymemcache.client.base import Client
from app.utilities.logs import (
    save_log_info,
    save_log_error)

EC_CACHE_MSG = 'ElasticCacheMessage'
EC_CACHE_ENV_KEY = 'config_ec_endpoint'


class Base5xxException(Exception):
    """
    Base server 500 exception
    """
    pass

class CacheAccessError(Base5xxException):
    """
    Cache access error, 5xx class error similar to internal error external connection failed
    """
    pass


class UndefinedCacheEndpointError(Base5xxException):
    """
    Undefined cache endpoint error, 5xx class error similar to internal error external connection failed
    """
    pass

class InvalidParametersException():
    """
    Invalid parameters exception
    """
    pass

class ECClient(object):
    """
    Standard wrapper class to access the elastic cache client.
    """

    def __init__(self, endpoint, is_local):

        # Do not need to actually use the parent class, this is a simple
        # delegate based on the constructor building the underlying Hash class
        # pylint: disable=W0231
        if not endpoint:
            raise InvalidParametersException(
                "Cannot create elastic cache client, endpoint input parameters must be declared")

        self._ec_endpoint = endpoint
        self._is_local = is_local
        self._wrapped_obj = self._get_elastic_cache_client()

    def __getattr__(self, attr):
        if hasattr(self._wrapped_obj, attr):
            val = getattr(self._wrapped_obj, attr)
        else:
            raise AttributeError('{0} does not exist'.format(attr))

        if callable(val):
            def _wrapped_func(*args, **kwargs):
                # this is where we can try to determine if an exception
                # would be a throttling error or not.
                kwargs = self._set_no_reply(attr, **kwargs)
                save_log_info(
                    'CacheOperation elastic cache {0} called with {1} {2}'.format(attr, args, kwargs))
                return_val = val(*args, **kwargs)
                save_log_info(
                    'CacheOperation elastic cache {0} returned {1}'.format(attr, return_val))
                return return_val
            return _wrapped_func
        return val

    @staticmethod
    def _set_no_reply(attr, **kargs):
        if not attr.startswith('get') and 'noreply' not in kargs:
            kargs['noreply'] = False
        return kargs

    def _get_elastic_cache_client(self):
        return ECClient._get_memcache_client(self._ec_endpoint, self._is_local)

    @staticmethod
    def _get_memcache_client(endpoint, is_local):
        client = None
        if is_local:
            address = endpoint.split(':')
            node = (address[0], int(address[1]))
            client = Client(
                node,
                serializer=ECClient._json_serializer,
                deserializer=ECClient._json_deserializer,
                timeout=10000,
                connect_timeout=10000)
        else:
            save_log_info('Trying to get nodes')
            nodes = elasticache_auto_discovery.discover(endpoint, 10)
            nodes = list(map(lambda x: (x[1], int(x[2])), nodes))
            # save_log_info(nodes)
            # address = endpoint.split(':')
            # nodes = [(address[0], int(address[1]))]
            save_log_info('Nodes found: {}'.format(nodes))
            client = HashClient(
                nodes,
                serializer=ECClient._json_serializer,
                deserializer=ECClient._json_deserializer,
                timeout=10000,
                connect_timeout=10000)

        save_log_info('Return ECClient: {}'.format(client))

        return client

    @staticmethod
    def _json_serializer(_key, value):
        if isinstance(value, str):
            return value, 1
        return json.dumps(value), 2

    @staticmethod
    def _json_deserializer(_key, value, flags):
        if flags == 2:
            return json.loads(value.decode('utf-8'))
        # Assume string
        return value


@ttl_cache(maxsize=4, ttl=120)
def get_ec_client(endpoint=None, is_local=False):
    """ Using decorator for closure, instances in decorator is now scoped
        outside the function. This function does no work it is all in the decorator.
    """
    if not endpoint and has_ec_endpoint_defined():
        endpoint = os.environ.get(EC_CACHE_ENV_KEY)
    if endpoint:
        try:
            return ECClient(endpoint=endpoint, is_local=is_local)
        except Exception as expt:
            msg = 'Elasticache client cannot be create: {0}'.format(expt)
            save_log_error(msg)
            raise CacheAccessError(expt) from expt
    else:
        raise UndefinedCacheEndpointError('Environment Variable {0} must be defined'.format(EC_CACHE_ENV_KEY))


def compress_cache_data(data):
    """
    Compress cache data in json form
    """
    if not is_dict_compressed(data):
        compress_out = {}
        compress_out['gz'] = compress_data(json.dumps(data))
        return compress_out
    return data


def decompress_cache_data(data):
    """
    Decompress cache data in json form
    """
    if is_dict_compressed(data):
        return json.loads(uncompress_data(data.get('gz')))
    return data


def is_dict_compressed(data):
    """
    Determines if the dictionary contains compressed data
    """
    if isinstance(data, dict):
        return 'gz' in data
    return False


def compress_data(rawdata, timestamp=0.):
    """
    Compress Raw Data
    """
    raw_bytes = io.BytesIO()
    with gzip.GzipFile(fileobj=raw_bytes, mode='w', mtime=timestamp) as f_compress:
        f_compress.write(rawdata.encode())
    return b64encode(raw_bytes.getvalue()).decode('utf-8')


def uncompress_data(compressed_data, timestamp=0.):
    """
    Uncompress data from compressed data
    """
    return gzip.GzipFile(fileobj=io.BytesIO(bytes(b64decode(compressed_data))), mtime=timestamp).read()


def has_ec_endpoint_defined():
    """
    Is cache endpoint defined as os environment variable
    """
    return EC_CACHE_ENV_KEY in os.environ.keys()
