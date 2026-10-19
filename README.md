# DFSha

> [!IMPORTANT]
> **Informe técnico:** [DFSha_Informe_Tecnico_v1.0.pdf](DFSha_Informe_Tecnico_v1.0.pdf).

DFSha es un sistema de archivos distribuido por bloques. Incluye servicios
NameNode para metadatos, DataNodes para almacenar bloques y un cliente Python
con biblioteca y CLI interactivo. Consulta también la
[arquitectura](docs/ARCHITECTURE.md), el [contrato de API](docs/API.md) y la
[guía de despliegue](docs/issues/09-docker-compose-and-cloud-deploy.md).

## Demo local con Docker

> **Solo desarrollo; no es seguro para producción.** Esta configuración usa
> HTTP, gRPC sin TLS, credenciales de demostración y no cifra los archivos de
> los DataNodes en disco. No la expongas a redes no confiables. TLS/mTLS,
> cifrado AES-256-GCM y failover automático de PostgreSQL siguen pendientes;
> consulta [issue 07](docs/issues/07-security-tls-and-encryption.md).

### 1. Requisitos y arranque

Necesitas Docker Desktop o Docker Engine con el plugin `docker compose`.
Los comandos siguientes se ejecutan desde la raíz del repositorio.

En PowerShell:

```powershell
Copy-Item .env.example .env
docker compose config
docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:8001/health
Invoke-RestMethod http://localhost:8002/health
```

En Linux, macOS, WSL o Git Bash:

```sh
cp .env.example .env
docker compose config
docker compose up --build -d
docker compose ps
curl --fail http://localhost:8001/health
curl --fail http://localhost:8002/health
```

Espera a que los servicios aparezcan como `running`/`healthy` en
`docker compose ps`. La primera construcción descarga imágenes y puede tardar.
La cuenta de demostración configurada en `.env.example` es
`demo-admin`; su contraseña es `local-demo-password-do-not-use`. En la demo
local no hace falta cambiar esos valores. **No uses estas credenciales fuera
de esta demo.** Si cambias `ADMIN_USER` o `ADMIN_PASSWORD` en `.env`, usa los
nuevos valores en los ejemplos de esta guía.

Para comprobar de inmediato una carga y descarga de más de un bloque:

```sh
docker compose exec -T client python -m scripts.smoke_test
```

El smoke test autentica con las variables de `.env`, sube y descarga un archivo,
y comprueba que los bytes y el SHA-256 coincidan.

### 2. Iniciar el CLI interactivo

Abre una terminal con TTY desde la raíz del repositorio:

```sh
docker compose exec -it client python -m client.cli --username demo-admin shell
```

Escribe la contraseña cuando el CLI la solicite. La sesión queda autenticada
hasta que escribas `logout` o `exit`; no se vuelve a pedir por cada operación.
En la sesión, escribe `help` para ver la ayuda y `exit` para salir. Las rutas
remotas relativas se resuelven desde el directorio que aparece en el indicador
de la consola (por ejemplo, `dfsha:/users/demo-admin$`).
`cd` cambia ese directorio; `cd` sin argumento vuelve al home del usuario.

### 3. Probar los comandos del CLI

`put` necesita que el archivo local exista dentro del contenedor `client`.
Para crear un archivo de prueba en el directorio `/tmp` del contenedor, ejecuta
en otra terminal:

```sh
docker compose exec -T client python -c "from pathlib import Path; Path('/tmp/demo.txt').write_text('archivo de prueba DFSha\n')"
```

Este archivo local `/tmp/demo.txt` es el contenido que subirá `put`. En cambio,
`touch vacio.txt` crea un archivo DFSha remoto vacío en el directorio remoto
actual; no crea ni prepara `/tmp/demo.txt`.

Ahora ejecuta esta secuencia dentro de la sesión interactiva, después del
indicador `dfsha:...$`:

```text
help
pwd
ls
mkdir -p practica
cd practica
touch vacio.txt
stat vacio.txt
put /tmp/demo.txt subida.txt
put /tmp/demo.txt subida.txt --overwrite
ls
get subida.txt /tmp/descargado.txt
mv subida.txt renombrado.txt
acl renombrado.txt
group equipo --create
group equipo --add demo-admin
group equipo
group equipo --remove demo-admin
group equipo --add demo-admin
acl renombrado.txt group:equipo rw
share renombrado.txt group:equipo r
lock renombrado.txt --mode exclusive --ttl 60
```

El primer `put` debe mostrar un resultado JSON con `"status": "committed"` y
crear `subida.txt` en el directorio remoto actual. El segundo comando vuelve a
subir el mismo contenido, esta vez solicitando explícitamente reemplazar el
archivo existente. `ls` debe mostrar `subida.txt`; después de `mv`, debe
mostrar `renombrado.txt`. Si `get` responde `FILE_NOT_FOUND`, comprueba que el
`put` anterior mostró `"status": "committed"` y que `ls` lista el archivo. Si
tu imagen no muestra el resultado del `put`, reconstruye y recrea el cliente
con `docker compose build client` y `docker compose up -d --no-deps client`,
vuelve a abrir el shell y repite la prueba.

`lock` imprime un objeto JSON con un `lock_id`. Copia ese valor y úsalo para
liberar el lock:

```text
unlock PEGA_AQUI_EL_LOCK_ID
```

Continúa la prueba y limpia los archivos/directorios remotos:

```text
rm vacio.txt
rm renombrado.txt
cd ..
rmdir practica
mkdir -p temporal/subdirectorio
touch temporal/subdirectorio/archivo.txt
rmdir -r temporal
exit
```

El archivo descargado está en `/tmp/descargado.txt` dentro del contenedor. Para
copiarlo de vuelta al directorio actual del host, ya fuera del CLI:

```sh
docker compose cp client:/tmp/descargado.txt ./descargado.txt
```

En PowerShell, crea el archivo de prueba en tu equipo y cópialo al contenedor
con una ruta que exista:

```powershell
Set-Content -NoNewline .\demo.txt "archivo de prueba DFSha"
docker compose cp .\demo.txt client:/tmp/demo.txt
```

Después de `exit`, recupera la descarga con:

```powershell
docker compose cp client:/tmp/descargado.txt .\descargado.txt
```

Para probar la autenticación y las API keys, vuelve a abrir el shell y ejecuta
estos comandos en la sesión interactiva:

```text
apikey create clave-de-prueba
apikey list
apikey revoke PEGA_AQUI_EL_KEY_ID
logout
login
```

`apikey create` devuelve un identificador y un secreto: usa el identificador
para revocar la clave y guarda el secreto solo si quieres probar una sesión
con `--api-key`. Al escribir `login`, el CLI vuelve a pedir usuario y
contraseña.

### 4. Referencia rápida de comandos

Estos comandos se escriben dentro de la sesión interactiva. Los argumentos
entre corchetes son opcionales; `LOCAL` es una ruta del contenedor y `REMOTO`
una ruta del sistema DFSha.

| Comando | Qué hace | Ejemplo |
| --- | --- | --- |
| `help`, `exit` (`quit`) | Muestra ayuda; termina la sesión. | `help` |
| `login`, `logout` | Inicia o termina la autenticación de la sesión. `login` vuelve a pedir usuario y contraseña. | `logout` y luego `login` |
| `pwd`, `ls [RUTA]`, `cd [RUTA]` | Muestra directorio, lista entradas y navega. `cd` sin ruta vuelve al home. | `ls`, `cd practica` |
| `stat RUTA` | Muestra metadatos de archivo o directorio. | `stat informe.txt` |
| `mkdir [-p] RUTA` | Crea un directorio; `-p` crea también los padres. | `mkdir -p proyectos/dfs` |
| `touch RUTA` | Crea un archivo remoto vacío. | `touch vacio.txt` |
| `rmdir [-r] RUTA` | Elimina un directorio vacío; `-r` permite eliminarlo recursivamente. | `rmdir proyectos` |
| `rm RUTA` | Elimina un archivo remoto. | `rm vacio.txt` |
| `mv ORIGEN DESTINO` | Mueve o renombra una ruta remota. | `mv viejo.txt nuevo.txt` |
| `put LOCAL REMOTO [--overwrite]` | Sube un archivo desde el contenedor cliente. | `put /tmp/demo.txt demo.txt` |
| `get REMOTO LOCAL` | Descarga un archivo al contenedor cliente. | `get demo.txt /tmp/demo.txt` |
| `acl RUTA` | Consulta la ACL. | `acl informe.txt` |
| `acl RUTA SUJETO PERMISO` | Actualiza la ACL. Permisos: `r`, `rw`, `none`. | `acl informe.txt user:ana r` |
| `share RUTA SUJETO PERMISO` | Alias para conceder o revocar acceso mediante ACL. | `share informe.txt group:equipo rw` |
| `lock RUTA [--mode shared\|exclusive] [--ttl SEGUNDOS]` | Adquiere un lock; guarda el `lock_id` que devuelve. | `lock informe.txt --mode exclusive --ttl 60` |
| `unlock LOCK_ID` | Libera el lock indicado. | `unlock ID_COPIADO` |
| `group NOMBRE --create` | Crea un grupo. | `group equipo --create` |
| `group NOMBRE [--add USUARIOS...] [--remove USUARIOS...]` | Consulta un grupo o modifica sus miembros. Los usuarios deben existir. | `group equipo --add demo-admin` |
| `apikey create [NOMBRE]` | Crea una API key; guarda el secreto mostrado porque no se vuelve a mostrar. | `apikey create laptop` |
| `apikey list` | Lista las API keys y sus identificadores. | `apikey list` |
| `apikey revoke KEY_ID` | Revoca una API key por su identificador. | `apikey revoke ID_COPIADO` |

Los comandos de transferencia distinguen entre el disco del host y el del
contenedor: `put` lee el archivo dentro de `client`; `get` escribe allí.
`docker compose cp` sirve para mover archivos entre ambos. `touch` crea un
archivo vacío nuevo; para reemplazar un archivo al subirlo, usa `put ... --overwrite`.
El CLI no ofrece escritura aleatoria en sitio: el modelo de escritura es WORM,
es decir, reemplazar el archivo completo.

Para probar los comandos administrativos, usa `group equipo --create` antes de
añadir miembros o asignar permisos al grupo. La demo solo crea automáticamente
el usuario administrador; `group ... --add` requiere que cada usuario ya exista.
`apikey create` enseña el secreto al crearlo: no lo compartas ni lo pegues en
repositorios.

### 5. Ejecutar un comando sin abrir el shell

También puedes ejecutar un comando en un proceso separado. Pon las opciones
globales antes del comando:

```sh
docker compose exec client python -m client.cli --username demo-admin ls /users/demo-admin
docker compose exec client python -m client.cli --username demo-admin mkdir /users/demo-admin/prueba
```

Cada proceso solicita la contraseña de nuevo. Para no repetir el login en cada
operación, usa el modo `shell`. También se puede autenticar con una API key
usando la opción global `--api-key` antes del comando.

## Biblioteca Python

El cliente también ofrece lectura por rangos y escritura WORM secuencial:

```python
from client import DFShaClient

client = DFShaClient("http://namenode1:8000")
try:
    client.login("demo-admin", password)
    with client.open("/users/demo-admin/archivo.txt", lock="shared") as remote_file:
        remote_file.seek(1024)
        data = remote_file.read(256)

    with client.open("/users/demo-admin/generado.bin", "wb") as remote_file:
        remote_file.write(b"primera parte")
        remote_file.write(b" y segunda parte")
finally:
    client.http.close()
```

`open()` en modo lectura obtiene solo los rangos solicitados. `open(..., "wb")`
guarda los bytes temporalmente y sube una versión completa al cerrar. No se
admiten modificaciones aleatorias en sitio. Para subir o bajar un archivo local
completo se pueden usar `put_file()` y `get_file()`.

## Topología, logs y apagado

La demo usa bloques de 1 MiB, factor de replicación 3, quorum de escritura de
dos réplicas y cinco DataNodes. Los servicios comparten una red privada de
Compose; por eso conviene ejecutar el cliente dentro del servicio `client`.
Los NameNodes y DataNodes escriben sus eventos de aplicación a los logs de
Docker. Sus mensajes llevan fecha, servicio y nivel; `WARNING` aparece en
amarillo, `ERROR` en rojo y los mensajes informativos en cian cuando la
terminal admite colores. Los checks exitosos de salud sí se muestran; solo se
ocultan los heartbeats y los informes de bloques exitosos, que son tráfico
frecuente entre nodos. Si sigues viendo líneas `httpx: HTTP Request ...
heartbeat ... 200 OK`, actualiza y recrea los contenedores con la versión
actual del código:

```sh
docker compose up --build -d namenode1 namenode2 datanode1 datanode2 datanode3 datanode4 datanode5
```

Este comando conserva los volúmenes. No uses `docker compose down -v` para
actualizar. En el servicio `client`, el shell interactivo muestra su propio
texto directamente en la terminal; sus mensajes no son logs de un servicio de
larga duración.

### Comprobar si los componentes están activos

Desde la raíz del repositorio, consulta el estado de todos los servicios:

```sh
docker compose ps
```

Busca `Up ... (healthy)` para los NameNodes, DataNodes y PostgreSQL. Si algún
servicio está `unhealthy`, `exited` o reiniciándose, inspecciona sus logs
individuales con los ejemplos de abajo. Además, los logs de `namenode1` y
`namenode2` muestran `GET /health ... 200`; cada DataNode muestra periódicamente
`Health OK` con el espacio libre y el número de bloques. PostgreSQL no imprime
cada `pg_isready` exitoso: su estado `healthy` aparece en `docker compose ps`.

### Ver logs individuales o por grupos

`--follow` mantiene la vista abierta; sal con `Ctrl+C`. `--tail=100` muestra
hasta 100 líneas anteriores por servicio antes de seguir los mensajes nuevos.
Elige un único servicio para aislarlo:

```sh
docker compose logs --follow --tail=100 namenode1
docker compose logs --follow --tail=100 namenode2
docker compose logs --follow --tail=100 datanode1
docker compose logs --follow --tail=100 postgres-primary
docker compose logs --follow --tail=100 postgres-replica
```

Para observar un grupo, pasa varios nombres de servicio:

```sh
docker compose logs --follow --tail=100 namenode1 namenode2
docker compose logs --follow --tail=100 datanode1 datanode2 datanode3 datanode4 datanode5
docker compose logs --follow --tail=100 postgres-primary postgres-replica
```

Para inspeccionar todos los componentes de la demo a la vez:

```sh
docker compose logs --follow --tail=100
```

Para buscar principalmente errores y eventos importantes en Linux, macOS,
WSL o Git Bash, deja el flujo abierto y filtra los mensajes:

```sh
docker compose logs --follow --tail=200 namenode1 namenode2 datanode1 datanode2 datanode3 datanode4 datanode5 | grep --line-buffered -Ei 'WARNING|ERROR|CRITICAL|dead|replic|stored block|failed|UNAVAILABLE'
```

En PowerShell, para revisar los mensajes recientes sin dejar el flujo abierto:

```powershell
docker compose logs --tail=300 namenode1 namenode2 datanode1 datanode2 datanode3 datanode4 datanode5 |
    Select-String -Pattern 'WARNING|ERROR|CRITICAL|dead|replic|stored block|failed|UNAVAILABLE'
```

Se puede probar la detección de una caída deteniendo un DataNode:

```sh
docker compose stop datanode1
docker compose logs --follow namenode1 namenode2
docker compose start datanode1
```

Los heartbeats e informes de bloques exitosos se omiten para reducir ruido;
los healthchecks exitosos, los errores HTTP y los avisos de fallos sí aparecen.
Cuando un DataNode almacena un bloque, sus logs muestran `Stored block` con el
ID y tamaño. Si rechaza una réplica, busca `WARNING` o `ERROR` y revisa la
dirección y el puerto del error.
La detección y reparación toman tiempo. PostgreSQL tiene una réplica hot-standby,
pero ambos NameNodes se conectan al primario: la promoción automática y el
cambio de DSN no están implementados. La promoción manual está documentada en
[issue 09](docs/issues/09-docker-compose-and-cloud-deploy.md).

Para apagar la demo conservando los datos:

```sh
docker compose down
```

**No ejecutes `docker compose down -v` si quieres conservar el estado:** ese
comando elimina los volúmenes con datos de PostgreSQL y de los bloques.

## Pruebas y lint

Con Python 3.13 y las dependencias instaladas en el entorno local:

```sh
python -m pytest -q
python -m ruff check client scripts/smoke_test.py server/nodes/namenode/api/routes/files.py server/nodes/namenode/api/routes/groups.py server/nodes/namenode/api/schemas.py server/nodes/namenode/tests/test_api.py
python -m unittest discover -s client/tests
```

Los tests que necesitan PostgreSQL leen la variable `POSTGRES_TEST_DSN`. Sin
ella, pytest los marca como omitidos; por eso puede aparecer un resumen como
`55 skipped`. No necesitas crear otra base ni publicar el puerto de PostgreSQL:
Compose ya levanta `postgres-primary`, y `namenode1` está conectado a su red
privada. Ejecuta la suite desde ese contenedor para que use la misma base
PostgreSQL del Compose:

```sh
docker compose exec -T namenode1 sh -lc 'POSTGRES_TEST_DSN="$POSTGRES_DSN" python -m pytest -q'
```

El comando reutiliza dentro del contenedor el DSN con el que trabaja el
NameNode, sin copiar la contraseña a la terminal ni exponer PostgreSQL al host.
Los tests crean un esquema temporal aislado en esa base y lo eliminan al
terminar; no apuntes `POSTGRES_TEST_DSN` a una base de datos de producción.
No ejecutes este comando mientras otra persona use la demo.

También puedes ejecutar la suite desde tu entorno local con `python -m pytest
-q`; en ese caso, los tests de PostgreSQL se omiten si no defines
`POSTGRES_TEST_DSN`. El contenedor `client` no está conectado a la red privada
de PostgreSQL, así que no uses `localhost:5432` desde tu equipo: el Compose no
publica ese puerto.

## Seguridad y despliegue

La configuración de Compose es un entorno de integración local, no una
instalación de producción. TLS para HTTP/gRPC, mTLS, cifrado AES-256-GCM,
reglas de firewall cloud y failover automático de PostgreSQL siguen pendientes.
Revisa [issue 07](docs/issues/07-security-tls-and-encryption.md) y
[issue 09](docs/issues/09-docker-compose-and-cloud-deploy.md) antes de planear
un despliegue fuera de la máquina local.
