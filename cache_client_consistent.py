import socket

from BloomFilter import BloomFilter
from consistentHash import ConsistentHash
from lru_cache import lru_cache
from node_ring import NodeRing
from pickle_hash import serialize_GET, serialize_PUT, serialize_DELETE
from sample_data import USERS
from server_config import NODES

BUFFER_SIZE = 1024


class UDPClient:

    def __init__(self, host, port):
        self.host = host
        self.port = int(port)


    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):


    def put():
        global hash_codes
        for u in USERS:
            data_bytes, key = serialize_PUT(u)
            response = client_ring.get_node(key).send(data_bytes)
            hash_codes.add(str(response.decode()))
            bloom.add(key)
            print(response)


    # GET all users.
    @lru_cache(5)
    def get(key):
        if bloom.is_member(key):
            response = client_ring.get_node(key).send(data_bytes)
        else:
            response = None
        return response

    global hash_codes
    hash_codes = set()
    put()
    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        print(get(key))


    def delete():
        global hash_codes
        for hc in hash_codes.copy():
            print(hc)
            data_bytes, key = serialize_DELETE(hc)
            if bloom.is_member(key):
                response = client_ring.get_node(key).send(data_bytes)
                print(response)
            else:
                print(None)
            hash_codes.remove(hc)

    delete()

if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    # client_ring = NodeRing(clients)
    client_ring = ConsistentHash(4, 6, clients)
    bloom = BloomFilter(20, 0.05)
    process(clients)
