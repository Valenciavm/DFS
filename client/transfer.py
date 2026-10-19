"""DataNode gRPC adapter boundary; exact protobuf schema is owned by DataNode team."""
from .exceptions import TransferNotConfiguredError

class GrpcBlockTransport:
    def __init__(self, *, ca_cert=None, client_cert=None, client_key=None, chunk_size=1024*1024, timeout=60):
        self.ca_cert, self.client_cert, self.client_key = ca_cert, client_cert, client_key
        self.chunk_size, self.timeout = chunk_size, timeout
    def write_block(self, **kwargs):
        raise TransferNotConfiguredError('BlockService .proto/generated stubs are not integrated yet; metadata REST works, block transfer needs the DataNode contract.')
    def read_block(self, **kwargs):
        raise TransferNotConfiguredError('BlockService .proto/generated stubs are not integrated yet; metadata REST works, block transfer needs the DataNode contract.')
