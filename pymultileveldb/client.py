import socket
from struct import pack, unpack
from msgpack import dumps

from request_palm import (
    MULTI_LEVELDB_GET, MULTI_LEVELDB_PUT, MULTI_LEVELDB_LOOKUP, MULTI_LEVELDB_INDEX,
    MULTI_LEVELDB_DUMP,
    GetRequest, PutRequest, AddIndex, LookupRequest, DumpDatabase,
    MULTI_LEVELDB_QUERY_RESP, MULTI_LEVELDB_STATUS_RESP, MULTI_LEVELDB_PUT_RESP,
    QueryResponse, StatusResponse, PutResponse, AddIndex,
)


packet_map = {
    MULTI_LEVELDB_QUERY_RESP : QueryResponse,
    MULTI_LEVELDB_STATUS_RESP : StatusResponse,
    MULTI_LEVELDB_PUT_RESP : PutResponse,
}

class MultiLevelDBClient(object):
    def __init__(self, host=None, port=None):
        if host:
            if port:
                self.connect(host, port)
            else:
                self.connect(host)

    def connect(self, host='localhost', port=4455):
        self.host = host
        self.port = port

        self.socket = socket.socket()
        self.socket.connect((host, port))

    def _send(self, mid, request):
        raw = request.dumps()
        self.socket.send(pack('ii', mid, len(raw)))
        self.socket.send(raw)

    def _recv(self, size):
        buf = ''
        while True:
            buf += self.socket.recv(size - len(buf))
            if len(buf) == size:
                return buf

    def _receive(self):
        typ, size = unpack('ii', self._recv(8))
        if not size:
            return None

        return packet_map[typ](self._recv(size))

    def _receiveQueryResponse(self):
        while True:
            packet = self._receive()
            print "***", packet.offset, len(packet.results), packet.total
            yield packet
            if packet.offset + len(packet.results) == packet.total:
                break

    def get(self, key):
        self._send(MULTI_LEVELDB_GET, GetRequest(key=key))
        return self._receive()

    def put(self, value):
        assert isinstance(value, dict), 'Values must be of type dict'
        self._send(MULTI_LEVELDB_PUT, PutRequest(value=dumps(value)))
        return self._receive()

    def addIndex(self, index):
        assert isinstance(index, list), "Index requests must be a list type"
        self._send(MULTI_LEVELDB_INDEX, AddIndex(field=dumps(index)))
        assert self._receive().status == StatusResponse.OKAY

    def dump(self):
        self._send(MULTI_LEVELDB_DUMP, DumpDatabase())
        return self._receiveQueryResponse()

    def lookup(self, query, offset=None, limit=None, allow_scan=None):
        assert isinstance(value, dict), 'Query must be of type dict'

        pb = LookupRequest(query=dumps(query))
        if offset: pb.offset = offset
        if limit: pb.limit = limit
        if allow_scan: pb.allow_scan = allow_scan

        self._send(MULTI_LEVELDB_LOOKUP, pb)
        return self._receiveQueryResponse()
