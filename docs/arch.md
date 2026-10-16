# Arquitectura del Sistema de Archivos Distribuido (DFS)

## 1. Visión General
Este sistema implementa un almacenamiento distribuido inspirado en HDFS, diseñado con una arquitectura cliente-servidor basada íntegramente en **gRPC** sobre **Python (`asyncio` / `grpc.aio`)** para garantizar alta concurrencia, rendimiento asíncrono y transferencia eficiente de bloques binarios sin cuellos de botella[cite: 1].

---

## 2. Componentes del Sistema

### A. Cliente (CLI)
* **Interfaz de Comandos:** Permite al usuario interactuar con el DFS para subir, descargar y listar archivos.
* **Cifrado:** Capa encargada de cifrar los datos antes de transmitirlos a los nodos[cite: 1].
* **Comunicación Directa:** Se conecta mediante gRPC tanto con el NameNode (metadatos) como directamente con los DataNodes (transferencia de bloques).

### B. ControlNode / NameNode
* **Metadatos:** Administra el mapa lógico de archivos, fragmentos y su ubicación en los DataNodes[cite: 1].
* **Lógica de Almacenamiento:** Gestiona las políticas de replicación (*replication number*), redundancia y el direccionamiento de nodos[cite: 1].
* **Autenticación y Cifrado:** Valida las operaciones de acceso de los clientes[cite: 1].
* **Control de Salud:** Recibe latidos (*heartbeats*) y reportes de bloques de los DataNodes.

### C. DataNodes
* **Almacenamiento Físico:** Guardan los bloques de datos (*chunks*) en directorios locales o contenedores[cite: 1].
* **Servidor gRPC:** Atiende solicitudes directas del cliente para escritura y lectura de bloques en paralelo.
* **Replicación en Pipeline:** Propaga las réplicas de los bloques hacia otros nodos para asegurar la redundancia exigida.

---

## 3. Protocolo de Comunicación
* **Tecnología:** **gRPC** (con HTTP/2 y Protocol Buffers) para todas las capas de comunicación (Cliente ⇄ NameNode, NameNode ⇄ DataNode, Cliente ⇄ DataNode).
* **Concurrencia:** Implementado con bucles asíncronos (`asyncio`) para evitar bloqueos de I/O en red y disco.

---

## 4. Estrategia de Particionamiento (Hash + Range)
* **Hash Partitioning:** Se aplica una función hash sobre la ruta o nombre del archivo para calcular un identificador único y distribuir uniformemente los bloques entre los DataNodes disponibles.
* **Range Partitioning:** Dentro del espacio de hash, los bloques se organizan en rangos estructurados para optimizar la búsqueda y recuperación secuencial o por intervalos.

---

## 5. Flujos de Operación

### Escritura (Upload)
1. El cliente solicita la creación del archivo al ControlNode enviando metadatos y factor de replicación.
2. El ControlNode calcula las particiones (Hash + Range) y asigna los DataNodes correspondientes.
3. El cliente transmite los bloques de datos **directamente** a los DataNodes por gRPC en paralelo.
4. Los DataNodes replican los bloques internamente según la política de redundancia.

### Lectura (Download)
1. El cliente solicita el archivo al ControlNode.
2. El ControlNode responde con el mapeo de bloques y qué DataNodes los almacenan.
3. El cliente abre canales gRPC directos con los DataNodes para descargar los fragmentos en paralelo.
4. El cliente reensambla los bloques para reconstruir el archivo original.

### Mantenimiento
* Los DataNodes envían *heartbeats* y reportes de bloques periódicos al ControlNode. Si un nodo falla, el ControlNode detecta la caída y ordena la re-replicación para mantener el factor de redundancia configurado.