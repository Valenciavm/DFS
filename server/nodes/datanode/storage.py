"""Local block storage on a DataNode's disk.

Blocks are plain files named ``<block_id>.dat`` inside one directory. Nothing
here knows about gRPC or the NameNode: it is the only layer that touches disk.
"""
import os
import shutil
import hashlib


class StorageManager:
    """Gestiona el almacenamiento físico de bloques binarios en el disco local."""

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir  # ruta de la carpeta donde se guardan los bloques
        os.makedirs(self.storage_dir, exist_ok=True)  # crea la carpeta si no existe

    def get_block_path(self, block_id: str) -> str:
        """Devuelve la ruta absoluta en disco del archivo del bloque."""
        return os.path.join(self.storage_dir, f"{block_id}.dat")

    def write_block(self, block_id: str, data: bytes) -> str:
        """Escribe un bloque completo en el disco y retorna su hash SHA-256."""
        path = self.get_block_path(block_id)  # obtiene la ruta de destino
        with open(path, "wb") as f:  # abre en modo binario de escritura
            f.write(data)  # escribe los bytes en disco
        return hashlib.sha256(data).hexdigest()  # retorna la huella sha-256 calculada

    def read_block(self, block_id: str) -> bytes:
        """Lee y devuelve los bytes del bloque desde el disco local."""
        path = self.get_block_path(block_id)  # ruta del bloque solicitado
        if not os.path.exists(path):  # valida si el archivo existe fisicamente
            raise FileNotFoundError(f"Bloque {block_id} no existe en este nodo.")
        with open(path, "rb") as f:  # lee en modo binario
            return f.read()

    def delete_block(self, block_id: str) -> bool:
        """Elimina un bloque huérfano o corrupto del disco local."""
        path = self.get_block_path(block_id)
        if os.path.exists(path):
            os.remove(path)  # elimina el archivo del sistema de archivos
            return True
        return False

    def get_disk_usage(self) -> tuple[int, int]:
        """Calcula el espacio total y usado en disco (en bytes)."""
        usage = shutil.disk_usage(self.storage_dir)  # consulta al sistema operativo
        return usage.total, usage.used  # retorna (capacidad_total, espacio_usado)
