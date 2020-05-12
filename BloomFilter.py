import inspect
import math

from pickle_hash import hash_code_hex


class BloomFilter:
    k = 0

    def __init__(self, n, fp):
        m = - (n * math.log(fp)) / (math.log(2) ** 2)
        global k
        k = (m / n) * math.log(2)
        k = int(k)
        global bf
        bf = [False] * int(m)

    def is_member(self, key):
        global bf
        size = len(bf)
        for i in range(k):
            val = hash_code_hex(key.encode('utf-8'))
            index = int(val, 16) % size
        if bf[index]:
            return True
        return False

    def add(self, key):
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        sth = False
        if 'deletee' in calframe[1][3]:
            sth = True
        global bf
        size = len(bf)
        for i in range(k):
            val = hash_code_hex(key.encode('utf-8'))
            index = int(val, 16) % size
        if sth:
            bf[index] = False
        else:
            bf[index] = True
        return True
