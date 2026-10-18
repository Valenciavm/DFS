"""gRPC contracts.

The ``*_pb2.py`` / ``*_pb2_grpc.py`` stubs are generated from the ``.proto``
files and are deliberately not committed (ARCHITECTURE.md section 10). Generate
them before running any node:

    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/block_service.proto

See docs/grpc_block_service.md.
"""
