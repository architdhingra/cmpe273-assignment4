from BloomFilter import BloomFilter
from pickle_hash import serialize_GET
from collections import OrderedDict


cache = OrderedDict()


def lru_cache(arg1):
    capacity = arg1

    def wrap(f):
        def wrapped_f(*args):
            global cache
            key = args[0]
            if cache.get(key):
                return cache.get(key)
            else:
                cache[key] = f(*args)
                cache.move_to_end(key)
                if len(cache) > capacity:
                    cache.popitem(last=False)
                return cache.get(key)
        return wrapped_f
    return wrap

    #
    # def lru_cache(cap):
    #     capacity = cap
    #     print("here 2")
    #
    #     def gett(func):
    #         print("here 3")
    #         hash_codes, client_ring = func()
    #         for hc in hash_codes:
    #             print(hc)
    #             data_bytes, key = serialize_GET(hc)
    #             if cache.get(key):
    #                 print(cache.get(key))
    #             else:
    #                 print(client_ring.get_node(key).send(data_bytes))
    #                 cache[key] = client_ring.get_node(key).send(data_bytes)
    #                 cache.move_to_end(key)
    #                 if len(cache) > capacity:
    #                     cache.popitem(last=False)
    #     return gett

    '''if bloom.is_member(key):
            if cache.get(key):
                print(cache.get(key))
                cache[key] = hc

        else:
            print(client_ring.get_node(key).send(data_bytes))'''


'''
    initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str) -> str:
        if key not in self.cache:
            return None
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: dict) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

        # RUNNER

    def delete(self, key: str):
        del self.cache[key]


def putt(func):
    keys, USERS = func()
    for count, u in enumerate(USERS):
        cache.put(keys[count], u)
        bloom.add(keys[count])


def gett(func):
    hash_codes, client_ring = func()
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        if bloom.is_member(key):
            if cache.get(key):
                print(cache.get(key))
        else:
            print(client_ring.get_node(key).send(data_bytes))


def deletee(func):
    keys = func()
    for key in keys:
        bloom.add(key)
        cache.delete(key)
'''
