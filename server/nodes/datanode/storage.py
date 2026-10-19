"""Local block storage on a DataNode's disk.

Blocks are plain files named ``<block_id>.dat`` inside one directory. Nothing
here knows about gRPC or the NameNode: it is the only layer that touches disk.
"""
import hashlib
import os
import shutil


class StorageManager:
    """Gestiona el almacenamiento físico de bloques binarios en el disco local."""

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir  # ruta de la carpeta donde se guardan los bloques
        os.makedirs(self.storage_dir, exist_ok=True)  # crea la carpeta si no existe

    def get_block_path(self, block_id: str) -> str:
        """Devuelve la ruta absoluta en disco del archivo del bloque."""
        return os.path.join(self.storage_dir, f"{block_id}.dat")

    def get_temp_block_path(self, block_id: str) -> str:
        """Devuelve la ruta de un archivo temporal antes de validar su integridad."""
        return os.path.join(self.storage_dir, f"{block_id}.tmp")  # ruta temporal mientras se reciben chunks

    def get_block_checksum_path(self, block_id: str) -> str:
        """Return the sidecar path containing the committed block size and hash."""
        return os.path.join(self.storage_dir, f"{block_id}.sha256")

    def count_blocks(self) -> int:
        """Cuenta los bloques definitivos almacenados en este nodo."""
        return len([f for f in os.listdir(self.storage_dir) if f.endswith(".dat")])

    def write_block(self, block_id: str, data: bytes) -> str:
        """Escribe un bloque completo en el disco y retorna su hash SHA-256."""
        path = self.get_block_path(block_id)  # obtiene la ruta de destino
        with open(path, "wb") as f:  # abre en modo binario de escritura
            f.write(data)  # escribe los bytes en disco
        checksum = hashlib.sha256(data).hexdigest()
        self.write_block_info(block_id, len(data), checksum)
        return checksum

    def read_block(self, block_id: str) -> bytes:
        """Lee y devuelve los bytes del bloque desde el disco local."""
        path = self.get_block_path(block_id)  # ruta del bloque solicitado
        if not os.path.exists(path):  # valida si el archivo existe fisicamente
            raise FileNotFoundError(f"Bloque {block_id} no existe en este nodo.")
        with open(path, "rb") as f:  # lee en modo binario
            return f.read()

    def get_block_info(self, block_id: str) -> tuple[int, str]:
        """Return a block's size and SHA-256 without loading it into memory."""
        path = self.get_block_path(block_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Bloque {block_id} no existe en este nodo.")

        checksum_path = self.get_block_checksum_path(block_id)
        if os.path.exists(checksum_path):
            try:
                with open(checksum_path, "r", encoding="ascii") as metadata:
                    size_text, checksum, modified_ns_text = metadata.read().split()
                size = int(size_text)
                int(checksum, 16)
                modified_ns = int(modified_ns_text)
            except (OSError, ValueError) as error:
                raise OSError(f"Invalid metadata for block {block_id}") from error
            stat = os.stat(path)
            if (
                stat.st_size != size
                or stat.st_mtime_ns != modified_ns
                or len(checksum) != 64
            ):
                raise OSError(f"Metadata does not match block {block_id}")
            return size, checksum

        hasher = hashlib.sha256()
        size = 0
        with open(path, "rb") as block_file:
            while chunk := block_file.read(1024 * 1024):
                hasher.update(chunk)
                size += len(chunk)
        checksum = hasher.hexdigest()
        self.write_block_info(block_id, size, checksum)
        return size, checksum

    def write_block_info(self, block_id: str, size: int, checksum: str) -> None:
        """Atomically persist metadata so ranged reads need not scan whole blocks."""
        temp_path = f"{self.get_block_checksum_path(block_id)}.tmp"
        modified_ns = os.stat(self.get_block_path(block_id)).st_mtime_ns
        with open(temp_path, "w", encoding="ascii") as metadata:
            metadata.write(f"{size} {checksum} {modified_ns}\n")
        os.replace(temp_path, self.get_block_checksum_path(block_id))

    def scan_blocks(self) -> list[dict[str, str | int]]:
        """Escanear bloques definitivos (*.dat) en el almacenamiento local.

        Excluye explícitamente archivos temporales (*.tmp) y sidecars (*.sha256).
        Devuelve una lista de diccionarios con formato requerido por NameNode:
        [{'block_id': str, 'size': int, 'checksum': str}, ...]
        """
        blocks: list[dict[str, str | int]] = []
        if not os.path.exists(self.storage_dir):
            return blocks

        for filename in os.listdir(self.storage_dir):
            if filename.endswith(".dat"):
                block_id = filename[:-4]
                try:
                    size, checksum = self.get_block_info(block_id)
                    blocks.append({
                        "block_id": block_id,
                        "size": size,
                        "checksum": checksum,
                    })
                except (OSError, FileNotFoundError):
                    # Si el archivo fue borrado concurrentemente o está inaccesible, se omite
                    continue
        return blocks

    def delete_block(self, block_id: str) -> bool:
        """Elimina un bloque huérfano o corrupto del disco local de forma idempotente."""
        deleted = False
        path = self.get_block_path(block_id)
        if os.path.exists(path):
            try:
                os.remove(path)
                deleted = True
            except OSError:
                pass

        checksum_path = self.get_block_checksum_path(block_id)
        if os.path.exists(checksum_path):
            try:
                os.remove(checksum_path)
            except OSError:
                pass

        temp_path = self.get_temp_block_path(block_id)
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass

        return deleted

    def get_disk_usage(self) -> tuple[int, int]:
        """Calcula el espacio total y usado en disco (en bytes)."""
        usage = shutil.disk_usage(self.storage_dir)  # consulta al sistema operativo
        return usage.total, usage.used  # retorna (capacidad_total, espacio_usado)
