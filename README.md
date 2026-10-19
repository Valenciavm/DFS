# DFSha

DFSha es un prototipo de sistema de archivos distribuido por bloques, inspirado
en HDFS. Separa el plano de control (NameNode y metadatos) del plano de datos
(DataNodes y bloques), con una API REST para operaciones de metadatos y gRPC
para transferir bloques.

> **Estado de `main`:** esta rama contiene el núcleo del NameNode, una API REST
> FastAPI y un servicio gRPC de DataNode. Todavía no es un despliegue completo:
> no incluye cliente/CLI de usuario, almacenamiento PostgreSQL, TLS/mTLS,
> cifrado de bloques en reposo ni Docker Compose. Consulta
> [la issue 09](docs/issues/09-docker-compose-and-cloud-deploy.md) para el
> alcance pendiente de despliegue; no asumas que sus pasos ya funcionan.

## Componentes actuales

| Componente | Estado en `main` |
|---|---|
| NameNode | Núcleo de metadatos en memoria, autenticación y ACL, planificación de bloques y API REST. |
| DataNode | Almacenamiento local de bloques y servicio gRPC; incluye transferencias en pipeline, lecturas por rango y validación de block tokens. |
| Cliente | No hay todavía una biblioteca de cliente ni una CLI de usuario implementadas. |
| Persistencia | El `MetadataStore` disponible es en memoria; PostgreSQL no está integrado. |
| Despliegue | No hay `docker-compose.yml`, imágenes ni guía de despliegue cloud. |
| Seguridad de transporte y almacenamiento | TLS/mTLS y cifrado de bloques en reposo están pendientes. Los block tokens y la autenticación de usuarios sí forman parte del código actual. |

Para el diseño y los contratos, consulta [Arquitectura](docs/ARCHITECTURE.md),
[API](docs/API.md), [modelos del NameNode](docs/NAMENODE_MODELS.md) y
[documentación del DataNode](docs/DATANODE.md).

## Requisitos

- Python 3.10 o posterior.
- `pip`.

El repositorio no declara una matriz de versiones de Python probadas. Las
dependencias directas están fijadas en [requirements.txt](requirements.txt).

## Preparar el entorno

Desde la raíz del repositorio, en PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

En Linux o macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Los stubs gRPC se generan localmente a partir del contrato protobuf y no se
guardan en Git:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/block_service.proto
```

Ejecuta ese comando desde la raíz del repositorio antes de iniciar o probar el
DataNode.

## Pruebas y lint

```powershell
python -m pytest -q
python -m ruff check .
```

La suite de replicación de bloques puede ejecutarse de forma aislada:

```powershell
python -m pytest tests/integration/test_block_replication.py -v
```

## Estado del arranque local

El punto de entrada del NameNode es `python -m server.nodes.namenode` y el del
DataNode es `python -m server.nodes.datanode`. El NameNode requiere al menos
`BLOCK_TOKEN_SECRET` y `JWT_SECRET` en el entorno. Sin embargo, el arranque del
servidor REST del NameNode no está verificado en esta revisión de `main`: el
código usa `settings.namenode_host` y `settings.namenode_port`, pero esos
atributos no están definidos en la configuración actual. Por eso este README
no presenta todavía una secuencia local de clúster como demo reproducible.

La ejecución completa de la issue 09 requiere además PostgreSQL primario y
réplica, dos NameNodes, cinco DataNodes, configuración de secretos y
certificados, scripts de comprobación y pruebas de humo. Esos archivos aún no
están en `main`.

## Issues y documentación

- [Issues pendientes y roadmap](docs/issues/README.md)
- [Plan de despliegue Docker Compose y cloud (issue 09)](docs/issues/09-docker-compose-and-cloud-deploy.md)
- [Plan de pruebas final (issue 10)](docs/issues/10-test-plan-execution-and-report.md)
- [Contrato REST y gRPC](docs/API.md)
- [Servicio gRPC de bloques](docs/GRPC_BLOCK_SERVICE.md)

## Seguridad

No uses secretos de producción en desarrollo ni subas credenciales, claves,
certificados privados o datos locales al repositorio. La configuración de
transporte TLS/mTLS y el cifrado en reposo siguen pendientes; el estado
implementado y las brechas conocidas están descritos en
[DATANODE.md](docs/DATANODE.md) y en la [issue 07](docs/issues/07-security-tls-and-encryption.md).
