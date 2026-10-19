# Plan de Implementación: Bucle de Control del DataNode e Integración REST con el NameNode (Issue 04)

## Contexto y Objetivos
Actualmente, el DataNode cuenta con su plano de datos gRPC (`BlockService`) completamente funcional, pero su plano de control en [`server/nodes/datanode/core.py`](file:///c:/Users/valen/OneDrive/Escritorio/DFS/server/nodes/datanode/core.py) solo tiene funciones simuladas con `print` para `register_with_namenode` y `heartbeat_loop`. Aún no interactúa con la API REST del NameNode para:
1. Registrarse (`POST /v1/nodes/register`).
2. Enviar latidos periódicos (heartbeats) y procesar comandos (`POST /v1/nodes/heartbeat`).
3. Enviar reportes de bloques escaneando archivos `.dat` en disco (`POST /v1/nodes/block-report`).
4. Reportar la recepción de bloques tras escrituras gRPC exitosas (`POST /v1/nodes/block-received`).
5. Notificar bloques corruptos detectados en lecturas o validaciones de integridad (`POST /v1/nodes/block-corrupt`).

---

## Arquitectura General

```
                  +----------------------------------------------+
                  |               NameNode (REST)                |
                  +----------------------------------------------+
                         ^             ^             ^
         Heartbeat (POST)|             |             | Reporte de Bloques (POST)
       Comandos (reply)  |             |             |
                         v             |             v
              +--------------------+   |    +--------------------+
              |   DataNode 1       |   |    |   DataNode 2       |
              | - Bucle Heartbeat  |   |    | - Bucle Heartbeat  |
              | - Bucle Reportes   |   |    | - Bucle Reportes   |
              | - Servidor gRPC    |   |    | - Servidor gRPC    |
              +--------------------+   |    +--------------------+
                         |             |             ^
                         |             |             |
           Replicar      |             |             |
          (Stream gRPC)  +---------------------------+
                         |
                         v (WriteBlock con x-block-token)
```

---

## Pasos Detallados de Implementación

### Paso 1: Mejoras en la Capa de Almacenamiento ([`storage.py`](file:///c:/Users/valen/OneDrive/Escritorio/DFS/server/nodes/datanode/storage.py))
- **Añadir `scan_blocks() -> list[dict]`**:
  - Escanear `self.storage_dir` filtrando exclusivamente archivos terminados en `*.dat`.
  - Excluir explícitamente archivos temporales (`*.tmp`), archivos de checksum (`*.sha256`) y subdirectorios.
  - Para cada bloque, extraer el `block_id` (removiendo `.dat`) y obtener su tamaño (`size`) y hash (`checksum`) usando `get_block_info(block_id)`.
  - Retornar una lista con la estructura: `[{"block_id": str, "size": int, "checksum": str}, ...]`.
- **Garantizar Idempotencia en `delete_block(block_id)`**:
  - Eliminar el `.dat`, su `.sha256`, y cualquier `.tmp` residual si existiera.
  - Retornar `True` si fue eliminado y `False` si ya no estaba presente, sin lanzar excepciones.

---

### Paso 2: Cliente REST Resiliente en el DataNode ([`core.py`](file:///c:/Users/valen/OneDrive/Escritorio/DFS/server/nodes/datanode/core.py))
- **Configuración y Gestión de Conexiones**:
  - Soporte para múltiples URLs de NameNodes (`namenode_urls: list[str]`).
  - Mantener seguimiento de `current_namenode_url` y del término actual del líder `current_term: int = 0`.
  - Administrar una instancia de `httpx.AsyncClient` con timeouts personalizados y reutilización de conexiones.
  - Inyectar encabezados obligatorios en cada petición:
    - `X-Node-Key` (clave secreta del nodo desde la configuración).
    - `X-Request-Id` (UUID4 único por solicitud).
- **Manejo de Errores y Failover**:
  - **`503 NOT_LEADER`**: Inspeccionar el JSON de respuesta buscando `leader_url`. Si se incluye, apuntar de inmediato a esa URL. Si no, rotar a la siguiente URL en `namenode_urls`.
  - **`409 NODE_NOT_REGISTERED`**: Disparar de inmediato un re-registro (`register_with_namenode()`), seguido inmediatamente por un reporte completo de bloques (`send_block_report()`).
  - **Timeout de Conexión / Red Inaccesible**: Reintentar con backoff exponencial acotado, rotando entre las URLs disponibles de NameNode.

---

### Paso 3: Bucles de Ciclo de Vida y Endpoints REST ([`core.py`](file:///c:/Users/valen/OneDrive/Escritorio/DFS/server/nodes/datanode/core.py))
- **`register_with_namenode()`**:
  - Consultar uso real de disco: `total, used = self.storage.get_disk_usage()`.
  - Payload: `{ "node_id": self.node_id, "internal_addr": self.internal_addr, "public_addr": self.public_addr, "capacity_bytes": total, "free_bytes": max(0, total - used) }`.
  - Endpoint: `POST /v1/nodes/register`.
  - Al recibir respuesta exitosa: actualizar `heartbeat_interval_s` y `block_report_interval_s` con los valores asignados por el NameNode.
- **`heartbeat_loop()`**:
  - Bucle continuo con intervalos de espera (`heartbeat_interval_s`).
  - Payload: `{ "node_id": self.node_id, "free_bytes": free, "used_bytes": used }`.
  - Endpoint: `POST /v1/nodes/heartbeat`.
  - **Control de Fencing por Término del Líder (`term`)**:
    - Si `term > self.current_term`:
      - Actualizar `self.current_term = term`.
      - Disparar de inmediato un reporte completo de bloques (`asyncio.create_task(self.send_block_report())`).
    - Si `term < self.current_term`:
      - Descartar la respuesta y no ejecutar los comandos recibidos (protección contra líderes zombis/desactualizados).
    - Si `term == self.current_term`:
      - Despachar los `commands` hacia `self.handle_commands(commands)`.
- **`send_block_report()` y `block_report_loop()`**:
  - Escanear los bloques locales en disco vía `self.storage.scan_blocks()`.
  - Payload: `{ "node_id": self.node_id, "blocks": blocks }`.
  - Endpoint: `POST /v1/nodes/block-report`.
  - Procesar cualquier comando que el NameNode devuelva en la respuesta.
  - El bucle periódico se ejecuta cada `block_report_interval_s` (por defecto cada 60s).
- **`notify_block_received(block_id: str, size: int, checksum: str)`**:
  - Payload: `{ "node_id": self.node_id, "block_id": block_id, "size": size, "checksum": checksum }`.
  - Endpoint: `POST /v1/nodes/block-received`.
  - Encolar notificaciones fallidas en memoria y reintentarlas en segundo plano para no perder confirmaciones.
- **`notify_block_corrupt(block_id: str)`**:
  - Eliminar la réplica local dañada vía `self.storage.delete_block(block_id)`.
  - Payload: `{ "node_id": self.node_id, "block_id": block_id }`.
  - Endpoint: `POST /v1/nodes/block-corrupt`.

---

### Paso 4: Despacho de Comandos y Replicación gRPC ([`core.py`](file:///c:/Users/valen/OneDrive/Escritorio/DFS/server/nodes/datanode/core.py))
- **`handle_commands(commands: list[dict])`**:
  - Si `cmd == "delete"`: invocar `self.storage.delete_block(block_id)`.
  - Si `cmd == "replicate"`:
    - Lanzar tarea asíncrona de replicación: `replicate_block_to_node(block_id, targets, token)`.
- **`replicate_block_to_node(block_id: str, targets: list[dict], token: str)`**:
  - Leer el bloque local en memoria usando `self.storage.read_block(block_id)`.
  - Para cada objetivo en `targets`:
    - Obtener la dirección interna del DataNode destino: `target.get("internal_addr")`.
    - Establecer canal gRPC hacia `internal_addr`.
    - Inicializar cliente `pb2_grpc.BlockServiceStub(channel)`.
    - Transmitir `WriteBlock` enviando el encabezado (con `token` en metadatos gRPC `(('x-block-token', token),)`) y los chunks del bloque.
    - Cerrar el canal gRPC limpiamente.

---

### Paso 5: Conectar `BlockServiceImpl` con Notificaciones gRPC ([`block_service.py`](file:///c:/Users/valen/OneDrive/Escritorio/DFS/server/nodes/datanode/block_service.py))
- En `BlockServiceImpl.WriteBlock()`:
  - Cuando la escritura local finaliza con éxito y el checksum coincide:
    ```python
    await self.dn.notify_block_received(
        header.block_id, bytes_received, computed_checksum
    )
    ```
- Cuando se detecta un bloque dañado en lecturas o validaciones de integridad:
  ```python
  await self.dn.notify_block_corrupt(block_id)
  ```

---

### Paso 6: Apagado Limpio (Graceful Shutdown) y Coordinación de Tareas
- Asegurar que `run()` ejecute concurrentemente con `asyncio.gather`:
  1. `self.start_grpc_server()`
  2. `self.heartbeat_loop()`
  3. `self.block_report_loop()`
  4. `self.outbox_flush_loop()` (reintento de notificaciones pendientes)
- Al recibir señal de cancelación o parada, detener limpiamente el servidor gRPC y cerrar el cliente `httpx.AsyncClient`.

---

### Paso 7: Suite Completa de Pruebas Automatizadas
Crear el directorio y tests en `server/nodes/datanode/tests/`:
- `test_storage.py`: Pruebas de `scan_blocks()`, exclusión de `.tmp`, y borrado idempotente.
- `test_control_loop.py`:
  - Registro contra aplicación ASGI / mock de NameNode.
  - Bucle de latidos y recepción/ejecución de comandos (`delete`, `replicate`).
  - Failover de líder (`503` con `leader_url`).
  - Fencing de términos (término mayor dispara reporte inmediato, término menor se descarta).
  - Re-registro automático al recibir `409 NODE_NOT_REGISTERED`.
- `test_integration_pipeline.py`:
  - Escritura gRPC hacia el DataNode que genera exitosamente la notificación `block-received` hacia el NameNode.
  - DataNode recibiendo orden de replicación del NameNode y transfiriendo el bloque a un segundo DataNode vía gRPC.

---

## Criterios de Aceptación
- [ ] La suite de pytest del DataNode pasa al 100%.
- [ ] No quedan métodos simulados o incompletos en `DataNode`.
- [ ] El fencing de términos y failover cumplen estrictamente con `API.md` Sección 7.
- [ ] Verificación de código limpia con linters y tipado estricto (`pytest` sin fallos).
