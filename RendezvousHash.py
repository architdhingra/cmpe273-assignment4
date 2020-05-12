from collections import defaultdict

import mmh3


class RendezvousHash(object):

    def __init__(self, nodes=None, seed=0):
        self.nodes = []
        self.seed = seed
        self.nodes = nodes
        self.hash = lambda x: mmh3.hash(x, seed)

    def add_node(self, node):
        self.nodes.append(node)

    def remove_node(self, node):
        self.nodes.remove(node)

    def get_node(self, key):
        high_score = -1
        n = None
        for node in self.nodes:
            score = self.hash("%s-%s" % (str(node), str(key)))
            print("Score: ", score)
            if score > high_score:
                (high_score, n) = (score, node)
            elif score == high_score:
                (high_score, n) = (score, max(str(node), str(n)))
        return n
