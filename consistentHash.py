import bisect
import hashlib


class ConsistentHash:

    def __init__(self, num_machines, num_replicas, nodes):
        self.nodes = nodes
        self.num_machines = num_machines
        self.num_replicas = num_replicas
        self.hash_tuples = self.calcTuple()

    def get_node(self, key):
        h = hash_it(key)
        if h > self.hash_tuples[-1][2]: return self.hash_tuples[0][0]
        hash_values = map(lambda x: x[2], self.hash_tuples)
        index = bisect.bisect_left(list(hash_values), h)
        idx = self.hash_tuples[index][0]
        return self.nodes[idx]

    def calcTuple(self):
        hash_tuples = [(j, k, hash_it(str(j) + "_" + str(k)))
                       for j in range(self.num_machines)
                       for k in range(self.num_replicas)]
        hash_tuples.sort(key=lambda x: x[2])
        print(hash_tuples)
        return hash_tuples


def hash_it(key):
    return (int(hashlib.md5(key.encode()).hexdigest(), 16) % 1000000) / 1000000.0
