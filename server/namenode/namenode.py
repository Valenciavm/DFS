import os
import asyncio
from server.namenode.models import MetadataStore
from dataclasses import dataclass


class Namenode:
    def __init__(self):
        self._store = MetadataStore()

    async def run(self):
        await asyncio.gather( 
            self.heartbeat(),#envia latidos a los datanodes para verificar si estan vivos.
            self.api_server(),#servidor que escucha las peticiones de los clientes y/o datanodes.
            self.garbage_collector(),#tarea periodica para verificar si hay bloques huérfanos. y limpiar la caché del datanode
        ) #en esta llamada se deben ejecutar las tareas que debe hacer el namenode de manera paralela. ej heartbeat

    async def api_server(self): 
        pass

    async def heartbeat(self): 
        pass

    async def replicate_blocks(self):
        pass

    async def garbage_collector(self):
        pass

    def save_file(self): #sync function to save file called by api_server()
        pass

if __name__ == "__main__":
    namenode = Namenode()
    asyncio.run(namenode.run())
        



