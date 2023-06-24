from nameko import config
from nameko.extensions import DependencyProvider
import redis

from products.exceptions import NotFound


REDIS_URI_KEY = 'REDIS_URI'


class StorageWrapper:
    """
    Product storage

    A very simple example of a custom Nameko dependency. Simplified
    implementation of products database based on Redis key value store.
    Handling the product ID increments or keeping sorted sets of product
    names for ordering the products is out of the scope of this example.

    """

    NotFound = NotFound

    def __init__(self, client):
        self.client = client

    def _format_key(self, product_id):
        return 'products:{}'.format(product_id)

    def _from_hash(self, document):
        return {
            'id': document[b'id'].decode('utf-8'),
            'title': document[b'title'].decode('utf-8'),
            'passenger_capacity': int(document[b'passenger_capacity']),
            'maximum_speed': int(document[b'maximum_speed']),
            'in_stock': int(document[b'in_stock'])
        }

    def get(self, product_id):
        product = self.client.hgetall(self._format_key(product_id))
        if not product:
            raise NotFound('Product ID {} does not exist'.format(product_id))
        
        return self._from_hash(product)

    def list(self, product_ids=[]):
        if product_ids == []:
            keys = self.client.scan_iter(count=1000)
        else:
            keys = {self._format_key(product_id) for product_id in product_ids}

        for key in keys:
            document = self.client.hgetall(key)
            if document:
                yield (self._from_hash(document))

    def create(self, product):
        self.client.hmset(
            self._format_key(product['id']),
            product)
        
    def delete(self, product_id):
        keys_removed = self.client.delete(self._format_key(product_id))
        if keys_removed == 0:
            raise NotFound('Product ID {} does not exist'.format(product_id))
        
        return {'id': product_id}

    def decrement_stock(self, product_id, amount):
        return self.client.hincrby(
            self._format_key(product_id), 'in_stock', -amount)


class Storage(DependencyProvider):

    def setup(self):
        self.client = redis.StrictRedis.from_url(config.get(REDIS_URI_KEY))

    def get_dependency(self, worker_ctx):
        return StorageWrapper(self.client)
