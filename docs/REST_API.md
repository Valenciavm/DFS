# Plan de Implementación: NameNode REST API (FastAPI) & Modelo de Seguridad

Este documento describe la arquitectura, el diseño técnico y el plan de implementación para la capa REST del NameNode (`server/nodes/namenode/api/`), así como el flujo integral de autenticación, control de accesos (ACL/RBAC) y la autorización de operaciones en el plano de datos mediante Block Tokens.

---

## 1. Arquitectura General y Separación de Responsabilidades

El NameNode separa estrictamente la lógica del sistema de archivos de la interfaz web:

- **La Regla de Oro**: La clase `Namenode` en `server/nodes/namenode/core.py` es código puro de Python. Contiene el árbol de inodos, el algoritmo de ubicación ponderada de bloques, recolección de basura, replicación y verificación de permisos. **No sabe nada de HTTP ni de FastAPI.**
- **La Capa FastAPI (`server/nodes/namenode/api/`)**: Es un **adaptador delgado (thin adapter)**. 
  - Recibe el request HTTP.
  - Extrae el usuario/token (`Authorization: Bearer ...`).
  - Llama al método correspondiente de `Namenode`.
  - Si hay un error (`DfshaError`), lo formatea en el estándar RFC 7807 (`API.md`).
  - Devuelve la respuesta en JSON.

### Clientes de esta API

La API REST atiende a **dos tipos de clientes muy diferentes**:

#### A. Los Clientes Humanos / Aplicaciones / CLI (El Sistema de Archivos)
1. **Autenticación (`/v1/auth/*`)**:
   - `/login`, `/register`, `/apikeys`. Devuelve el JWT de sesión.
2. **Archivos y Plan de Subida (`/v1/files/*`)**:
   - `POST /v1/files/put`: El cliente dice *"quiero subir un archivo de 200MB"*. El NameNode calcula cuántos bloques se necesitan (ej. 4 bloques de 64MB) y responde con un **UploadPlan**: a qué DataNodes enviarlos y los tokens HMAC (`x-block-token`) para poder escribir.
   - `POST /v1/files/commit`: Cuando el cliente termina de subir los bloques a los DataNodes por gRPC, le avisa al NameNode: *"listo, ya subí todo con estos hashes, regístralo"*.
   - `GET /v1/files/get`: Para descargar. El NameNode responde qué DataNodes tienen cada bloque.
3. **Árbol de Directorios (`/v1/fs/*`)**:
   - `ls`, `mkdir`, `rm`, `stat`, `mv`, control de permisos `acl` y bloqueos de archivo (`locks`).

#### B. Los DataNodes (El Control Loop Interno)
1. **Nodos (`/v1/nodes/*`)**:
   - `POST /v1/nodes/register`: Cuando un DataNode arranca, se registra: *"Hola, soy el DataNode 1, mi IP gRPC es X, y tengo 500GB libres"*.
   - `POST /v1/nodes/heartbeat`: Cada 3 segundos: *"Sigo vivo, tengo X espacio"*. El NameNode le responde con órdenes (ej. *"elimina el bloque B123"*, o *"replica el bloque B456 al DataNode 3"*).
   - `POST /v1/nodes/block-report`: Cada 30 segundos: *"Tengo esta lista completa de bloques en disco"*.
   - `POST /v1/nodes/block-received`: *"Acabo de recibir y guardar con éxito el bloque B123"*.

#### C. Administración y Monitoreo (`/v1/admin/*`, `/v1/ha/*`, `/health`)
- Métricas del cluster (espacio usado, réplicas sanas vs degradadas, uptime, estado de líder).

---

## 2. Estructura de Módulos (`server/nodes/namenode/api/`)

```text
server/nodes/namenode/api/
├── __init__.py
├── app.py          # Factoría create_app(), middlewares (X-Request-Id), exception handlers centralizados
├── deps.py         # Inyección de dependencias (extracción de usuario vía JWT/API Key, instancia de Namenode)
├── schemas.py      # Modelos Pydantic para Requests y Responses según API.md
└── routes/
    ├── __init__.py
    ├── auth.py     # /v1/auth/* (register, login, apikeys)
    ├── fs.py       # /v1/fs/* (ls, stat, mkdir, rmdir, rm, mv, acl, locks)
    ├── files.py    # /v1/files/* (put, commit, abort, get, tokens)
    ├── nodes.py    # /v1/nodes/* (register, heartbeat, block-report, block-received, block-corrupt)
    ├── admin.py    # /v1/admin/* y /health
    └── ha.py       # /v1/ha/status
```

---

## 3. Flujo Completo de Autenticación y Seguridad

El sistema maneja **dos niveles de seguridad complementarios**:
1. **Seguridad a nivel de Usuario / NameNode (Control Plane)**: Para saber quién eres y a qué carpetas o archivos tienes permiso. Se maneja con **JWT o API Keys**.
2. **Seguridad a nivel de DataNode / Bloques (Data Plane)**: Para que nadie pueda leer o escribir bloques sueltos directamente en los DataNodes sin autorización previa del NameNode. Se maneja con **Block Tokens (HMAC)**.

### Paso 1: Registro e Inicio de Sesión (Obtención de Identidad)

1. **El usuario se registra**:
   - Endpoint: `POST /v1/auth/register` con cuerpo `{"username": "ana", "password": "mi-password-seguro"}`.
   - El NameNode hashea la contraseña usando **Argon2id** (nunca se guarda en texto plano), crea el usuario con rol `"user"` en el `auth_store`, y le crea automáticamente su directorio personal `/users/ana/` donde ella es la dueña (`owner`).
   - Retorna: `201 Created`.

2. **El usuario inicia sesión**:
   - Endpoint: `POST /v1/auth/login` con `{"username": "ana", "password": "mi-password-seguro"}`.
   - El NameNode verifica el hash en Argon2. Si coincide, emite un **JWT firmado** con una clave secreta (`JWT_SECRET`) que contiene:
     - El sujeto (`sub: "ana"`).
     - Su rol (`role: "user"`).
     - Tiempo de expiración (ej. 1 hora).
   - Retorna: `{"access_token": "eyJhbGciOi...", "token_type": "bearer", "expires_in": 3600}`.
   *(Nota: si el usuario prefiere automatizaciones o scripts, puede usar `POST /v1/auth/apikeys` para obtener un `X-API-Key` permanente en lugar del JWT).*

### Paso 2: Ejecución de Operaciones en el Filesystem (Validación de Permisos)

Cada vez que "ana" quiera hacer algo en el sistema de archivos:
- Ella envía su header HTTP: `Authorization: Bearer eyJhbGciOi...`

1. **Intercepción en FastAPI (`deps.py`)**:
   - Antes de que el código del endpoint se ejecute, una dependencia de FastAPI intercepta la petición.
   - Valida la firma criptográfica y la expiración del JWT.
   - Extrae el usuario (`ana`) y lo coloca en el contexto actual (`current_principal.set(user)`).

2. **Llamada al Endpoint y Verificación ACL**:
   - Ejemplo 1: `POST /v1/fs/mkdir` con `{"path": "/users/ana/proyectos"}`.
     - Como la ruta padre `/users/ana/` tiene como `owner` a "ana", el NameNode permite la creación y responde `201 Created`.
   - Ejemplo 2: `POST /v1/fs/mkdir` con `{"path": "/users/carlos/secreto"}`.
     - El NameNode consulta los metadatos de `/users/carlos/`. El dueño es "carlos".
     - El NameNode verifica si "ana" está en alguna ACL de permisos compartidos para esa carpeta. Como no lo está (y "ana" no es admin), el NameNode lanza de inmediato un error `403 Forbidden` (`PERMISSION_DENIED`).

### Paso 3: Subida de Archivos y el Salto a los DataNodes (Block Tokens)

Aquí es donde se conectan el mundo REST y el mundo gRPC:

1. **Ana solicita subir un archivo**:
   - Endpoint: `POST /v1/files/put` con:
     `{"path": "/users/ana/video.mp4", "size": 134217728}` (128 MB = 2 bloques de 64 MB).
   - El NameNode verifica que "ana" tenga permiso de escritura en esa carpeta.
   - Si tiene permiso, el NameNode calcula la ubicación:
     - Bloque 1 (`blk_001`): lo asigna al pipeline `[DataNode-1, DataNode-2]`.
     - Bloque 2 (`blk_002`): lo asigna al pipeline `[DataNode-3, DataNode-4]`.
   - **La clave de seguridad aquí**: Para cada bloque, el NameNode firma criptográficamente un **Block Token** (usando HMAC-SHA256 con una clave secreta compartida entre NameNode y DataNodes).
     - El token dice: *"Se autoriza a 'ana' a ESCRIBIR el bloque 'blk_001' de máximo 64MB durante los próximos 15 minutos"*.
   - El NameNode responde a Ana con el **UploadPlan**, que contiene los DataNodes asignados y su respectivo `token` para cada bloque.

2. **Ana envía los datos a los DataNodes (por gRPC)**:
   - Ana abre una conexión gRPC directa hacia el DataNode-1 llamando a `WriteBlock`.
   - En los metadatos de la llamada gRPC, envía: `x-block-token: <el token que le dio el NameNode>`.
   - El interceptor gRPC del DataNode-1 valida la firma HMAC del token antes de aceptar cualquier byte. Si el token es inválido o expiró, rechaza la conexión con `UNAUTHENTICATED`.
   - De esta forma, **ningún usuario puede inyectar datos o alterar bloques en los DataNodes sin haber pasado primero por el NameNode**.

3. **Confirmación en el NameNode**:
   - Una vez que los DataNodes guardan los bloques, Ana vuelve a la API REST del NameNode:
   - Endpoint: `POST /v1/files/commit` con los hashes SHA-256 de los bloques.
   - El NameNode valida los datos y registra el archivo definitivamente en el sistema de archivos.

### Paso 4: Descarga de Archivos

El flujo de lectura es el espejo de la subida:
1. Ana pide leer: `GET /v1/files/get?path=/users/ana/video.mp4` (con su JWT en el header).
2. El NameNode verifica que Ana tenga permiso de lectura (`r`) en ese archivo.
3. El NameNode responde con la lista de bloques, qué DataNodes tienen réplicas vivas de cada bloque, y un **Block Token de LECTURA** para cada uno.
4. Ana se conecta por gRPC a cualquier DataNode de la lista llamando a `ReadBlock` con su token de lectura.
5. El DataNode valida el token de lectura y le transmite los bytes.

---

## 4. Fases de Implementación del Issue 02

### Fase 1: Base de la API, Errores y Dependencias (`app.py`, `deps.py`, `schemas.py`)
- **Middleware `X-Request-Id`**: Capturar o autogenerar `uuid4()` y propagarlo en cabeceras de respuesta y logs.
- **Handler Global de Excepciones**: Capturar `DfshaError` y devolver automáticamente `JSONResponse(status_code=e.http_status, content=e.to_body())` (RFC 7807).
- **Inyección de Dependencias (`deps.py`)**: `get_namenode()` y `get_current_principal()` (Bearer JWT o `X-API-Key`).
- **Modelos Pydantic (`schemas.py`)**: Request/Response DTOs según `docs/API.md`.

### Fase 2: Rutas de Nodos y Archivos (`routes/nodes.py`, `routes/files.py`)
- **Rutas de DataNodes (`routes/nodes.py`)**:
  - `POST /v1/nodes/register` -> `Namenode.register_datanode()`
  - `POST /v1/nodes/heartbeat` -> `Namenode.record_heartbeat()`
  - `POST /v1/nodes/block-report` -> `Namenode.process_block_report()`
  - `POST /v1/nodes/block-received` -> `Namenode.record_block_received()`
  - `POST /v1/nodes/block-corrupt` -> `Namenode.record_block_corrupt()`
- **Rutas de Archivos (`routes/files.py`)**:
  - `POST /v1/files/put` -> `Namenode.prepare_file_upload()`
  - `POST /v1/files/commit` -> `Namenode.commit_file_upload()`
  - `POST /v1/files/abort` -> `Namenode.abort_file_upload()`
  - `GET /v1/files/get` -> `Namenode.get_file_blocks()`
  - `POST /v1/files/tokens` -> renovación de tokens de bloque.

### Fase 3: Filesystem, Permisos y Locks (`routes/fs.py`, `routes/auth.py`)
- **Rutas de Autenticación (`routes/auth.py`)**:
  - `POST /v1/auth/register`, `POST /v1/auth/login`, `POST /v1/auth/apikeys`.
- **Rutas del Filesystem (`routes/fs.py`)**:
  - `GET /v1/fs/ls` (con paginación `limit`/`cursor`), `GET /v1/fs/stat`.
  - `POST /v1/fs/mkdir`, `DELETE /v1/fs/rmdir`, `DELETE /v1/fs/rm`.
  - `POST /v1/fs/mv`.
  - `GET /v1/fs/acl`, `PUT /v1/fs/acl`.
  - `POST /v1/fs/lock`, `POST /v1/fs/lock/{id}/renew`, `DELETE /v1/fs/lock/{id}`.

### Fase 4: Admin, HA Status e Integración Uvicorn (`routes/admin.py`, `routes/ha.py`)
- `GET /health` -> estado del nodo.
- `GET /v1/admin/stats` -> métricas y capacidad del cluster.
- `GET /v1/ha/status` -> estado de liderazgo (inicialmente líder fijo hasta Issue 06).
- **Arranque asíncrono**: En `server/nodes/namenode/core.py`, reemplazar el placeholder `await asyncio.Event().wait()` en `Namenode.api_server` para lanzar Uvicorn compartiendo el event loop con los monitores en background.

---

## 5. Estrategia de Pruebas
- Pruebas unitarias y de integración de la API usando `httpx.AsyncClient` o `starlette.testclient.TestClient` en `server/nodes/namenode/tests/test_api_*.py`.
- Cobertura completa de respuestas exitosas (200, 201, 204) y errores esperados (400, 401, 403, 404, 409, 423).
- Ejecución aislada con reloj simulado y almacenamiento en memoria, sin requerir infraestructura externa para CI.
