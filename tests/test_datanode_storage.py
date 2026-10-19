import os
import pytest
from server.nodes.datanode.storage import StorageManager


def test_storage_scan_blocks(tmp_path):
    storage = StorageManager(str(tmp_path))

    # Escribir 2 bloques definitivos
    b1_checksum = storage.write_block("blk_1", b"hello world 1")
    b2_checksum = storage.write_block("blk_2", b"hello world 2")

    # Crear archivo temporal que debe ser ignorado
    temp_file = storage.get_temp_block_path("blk_tmp")
    with open(temp_file, "wb") as f:
        f.write(b"in progress upload")

    # Crear archivo arbitrario
    with open(os.path.join(str(tmp_path), "notes.txt"), "w") as f:
        f.write("ignore me")

    scanned = storage.scan_blocks()
    assert len(scanned) == 2

    scanned_dict = {b["block_id"]: b for b in scanned}
    assert "blk_1" in scanned_dict
    assert scanned_dict["blk_1"]["size"] == len(b"hello world 1")
    assert scanned_dict["blk_1"]["checksum"] == b1_checksum

    assert "blk_2" in scanned_dict
    assert scanned_dict["blk_2"]["size"] == len(b"hello world 2")
    assert scanned_dict["blk_2"]["checksum"] == b2_checksum


def test_storage_delete_block_idempotent(tmp_path):
    storage = StorageManager(str(tmp_path))
    storage.write_block("blk_del", b"delete me")

    # Crear también un archivo .tmp residual
    temp_file = storage.get_temp_block_path("blk_del")
    with open(temp_file, "wb") as f:
        f.write(b"leftover temp")

    assert os.path.exists(storage.get_block_path("blk_del"))
    assert os.path.exists(storage.get_block_checksum_path("blk_del"))
    assert os.path.exists(temp_file)

    # Primer borrado: retorna True y borra .dat, .sha256 y .tmp
    assert storage.delete_block("blk_del") is True
    assert not os.path.exists(storage.get_block_path("blk_del"))
    assert not os.path.exists(storage.get_block_checksum_path("blk_del"))
    assert not os.path.exists(temp_file)

    # Segundo borrado sobre el mismo bloque: es idempotente y retorna False sin lanzar excepción
    assert storage.delete_block("blk_del") is False
