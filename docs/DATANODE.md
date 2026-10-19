# Estado del DataNode: Capacidades Actuales y Pendientes

Documento de análisis del estado del **DataNode** en la arquitectura de DFSha, evaluando sus responsabilidades, capacidades implementadas y pendientes de cara a los requisitos del sistema.

---

## 1. Capacidades Actuales (Lo que el DataNode puede hacer hoy)

El DataNode cuenta con sus dos planos fundamentales completamente operativos e integrados:

### A. Plano de Datos (Data Plane - gRPC)
- **Escritura en Pipeline (`WriteBlock`)**:
  - Recepción de bloques binarios mediante streaming desde clientes u otros DataNodes.
  - Reenvío concurrente de chunks hacia el siguiente nodo de la cadena (`pipeline`).
  - Aislamiento de nodos downstream caídos (`failed_downstream`) sin comprometer el almacenamiento de la copia local.
  - Verificación independiente del hash SHA-256 de los bytes recibidos contra el encabezado antes de consolidar el archivo en disco.
  - Transición atómica de archivo temporal (`.tmp`) a bloque definitivo (`.dat`) y creación de metadata sidecar (`.sha256`) con tamaño y hash.
- **Lectura por Rangos (`ReadBlock`)**:
  - Lectura eficiente con `offset` y `length` arbitrarios (permite lecturas y accesos aleatorios sin cargar el bloque entero en memoria RAM).
  - Verificación de integridad SHA-256 cuando se lee el bloque completo.
- **Consultas de Bloque y Salud (`StatBlock`, `Health`)**:
  - `StatBlock`: Retorna el tamaño y checksum de un bloque local sin transferir el contenido binario.
  - `Health`: Reporta estado (`ok`/`degraded`), espacio libre en disco y cantidad de bloques almacenados.
- **Autenticación por Token gRPC (`BlockTokenInterceptor`)**:
  - Validación de tokens criptográficos HMAC (`x-block-token`) firmados por el NameNode en cada llamada a `WriteBlock` y `ReadBlock`, verificando expiración y operación permitida.
- **Replicación Dirigida (`replicate_block_to_node`)**:
  - Funciona como cliente gRPC para leer un bloque local y transferirlo por streaming a otro DataNode cuando el NameNode lo instruye.

### B. Plano de Control y Almacenamiento (Control Plane - REST + Almacenamiento Local)
- **Registro Automático (`POST /v1/nodes/register`)**:
  - Consulta y calcula la capacidad total y espacio libre real en disco (`shutil.disk_usage`).
  - Publica direcciones independientes para red interna entre DataNodes (`internal_addr`) y red pública para clientes (`public_addr`).
  - Negocia dinámicamente los intervalos de latidos y reportes asignados por el NameNode.
- **Latidos y Comandos (`POST /v1/nodes/heartbeat`)**:
  - Envío de latidos periódicos automáticos con espacio libre y usado actualizado.
  - Recepción y ejecución de órdenes de borrado (`delete`) y replicación (`replicate`).
- **Fencing de Liderazgo por Término (`term`)**:
  - Descarta comandos provenientes de términos inferiores (protección contra órdenes de líderes desactualizados o zombis).
  - Si detecta un término mayor, actualiza su término y dispara de inmediato un reporte completo de bloques para que el nuevo líder reconstruya el mapa de réplicas en memoria.
- **Inventario y Escaneo de Disco (`scan_blocks` & `POST /v1/nodes/block-report`)**:
  - Escaneo estricto de archivos definitivos `.dat`, ignorando temporales `.tmp` de transferencias en curso y archivos `.sha256`.
  - Envío periódico del inventario completo de réplicas hacia el NameNode.
- **Notificaciones en Tiempo Real**:
  - Disparo de `POST /v1/nodes/block-received` al completar exitosamente una escritura gRPC.
  - Disparo de `POST /v1/nodes/block-corrupt` ante fallos de lectura o integridad, eliminando automáticamente la copia local dañada.
  - Cola en memoria (outbox) con bucle de reintento (`outbox_flush_loop`) ante cortes de red transitorios con el NameNode.
- **Borrado Idempotente (`delete_block`)**:
  - Eliminación segura de archivos `.dat`, `.sha256` y cualquier `.tmp` residual sin arrojar excepciones si el bloque ya fue retirado.

---

## 2. Capacidades Pendientes (Lo que le falta al DataNode)

Las siguientes características corresponden a los issues de seguridad e infraestructura transversal del sistema:

### A. Cifrado en Reposo (AES-256-GCM at rest — Issue 07)
*Actualmente los bloques se guardan como texto plano en disco.*
- **Requisitos:**
  - Configurar `DATANODE_MASTER_KEY` (32 bytes aleatorios codificados en Base64).
  - Derivar una clave única por bloque utilizando HKDF-SHA256 con el `block_id` como salt/info.
  - Cifrar el bloque con AES-256-GCM y un nonce aleatorio fresco de 96 bits antes de persistirlo en el archivo `.dat`.
  - Almacenar encabezado versionado que incluya: magic, versión, nonce, tamaño en texto plano y authentication tag GCM.
  - Validar el authentication tag GCM en cada lectura (`ReadBlock`); ante un fallo de autenticación, responder con `DATA_LOSS`, notificar `block-corrupt` y eliminar la copia local dañada.

### B. Cifrado en Tránsito y mTLS (TLS/mTLS — Issue 07)
*Actualmente gRPC utiliza canales inseguros y HTTP no cuenta con certificados mutuos.*
- **Requisitos:**
  - Habilitar TLS en el servidor gRPC usando certificados de servidor (`grpc.ssl_server_credentials`).
  - **mTLS entre DataNodes:** En llamadas de replicación interna DataNode ↔ DataNode, autenticarse mutuamente con certificados de cliente validados contra la Autoridad Certificadora (CA) del clúster.
  - Validar los certificados del NameNode contra la CA del clúster en las conexiones HTTPS salientes.

### C. Métrica de Transferencias Activas (`active_transfers`)
- Contabilizar los streams concurrentes de lectura y escritura activos para enviar `active_transfers` en el payload del heartbeat (`API.md` Sección 7), facilitando un balanceo de carga óptimo por parte del NameNode.

### D. Scrubber de Disco en Segundo Plano (Opcional / Deseable)
- Tarea en segundo plano que recorra periódicamente los bloques en disco verificando su checksum SHA-256 y tag GCM para detectar *bit rot* (corrupción silenciosa de hardware) de forma proactiva antes de que un cliente intente leer el bloque.
