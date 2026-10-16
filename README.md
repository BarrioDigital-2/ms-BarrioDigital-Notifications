# ms-barriodigital-notifications

Microservicio **consumidor** de BarrioDigital. Escucha en RabbitMQ los eventos de solicitudes vecinales (creación y cambio de estado) y genera las notificaciones. Los mensajes que no se pueden procesar terminan en **Dead Letter Queues (DLQ)**, donde se registran.

Incluye además el `docker-compose.yml` del **cluster RabbitMQ de 2 nodos** que usa todo el sistema.

| | |
|---|---|
| Puerto | `8082` |
| Stack | Java 17, Spring Boot 4.1.1, Spring AMQP 4.1, RabbitMQ 4.2 |
| Productor de los eventos | `ms-barriodigital-requests` |

---

## Topología

```
                          ┌─ solicitud.created ─▶ solicitudes.creadas ──────────▶ SolicitudCreadaConsumer
  solicitudes.direct ─────┤                            │ x-dead-letter-exchange
     (Direct)             └─ solicitud.status ──▶ solicitudes.estados ──────────▶ SolicitudEstadoConsumer
                                                       │
  solicitudes.topic ──── solicitud.# ──────────▶ solicitudes.notificaciones ───▶ NotificacionGeneralConsumer
     (Topic)                                           │
                                                       ▼  NACK o TTL vencido
                          solicitudes.creadas.dlx ─────────▶ solicitudes.creadas.dlq ───────┐
                          solicitudes.estados.dlx ─────────▶ solicitudes.estados.dlq ───────┼──▶ DlqConsumer
                          solicitudes.notificaciones.dlx ──▶ solicitudes.notificaciones.dlq ┘
                                (Fanout)
```

### Exchanges

| Exchange | Tipo | Uso |
|----------|------|-----|
| `solicitudes.direct` | Direct | Enruta cada evento a su cola específica por routing key exacta |
| `solicitudes.topic` | Topic | Enruta todos los eventos de solicitud (`solicitud.#`) a la cola general |
| `solicitudes.creadas.dlx` | Fanout | Dead Letter Exchange de `solicitudes.creadas` |
| `solicitudes.estados.dlx` | Fanout | Dead Letter Exchange de `solicitudes.estados` |
| `solicitudes.notificaciones.dlx` | Fanout | Dead Letter Exchange de `solicitudes.notificaciones` |

### Colas

| Cola | Binding | Argumentos |
|------|---------|------------|
| `solicitudes.creadas` | `solicitudes.direct` + `solicitud.created` | TTL 30 s, DLX `solicitudes.creadas.dlx` |
| `solicitudes.estados` | `solicitudes.direct` + `solicitud.status` | TTL 30 s, DLX `solicitudes.estados.dlx` |
| `solicitudes.notificaciones` | `solicitudes.topic` + `solicitud.#` | TTL 30 s, DLX `solicitudes.notificaciones.dlx` |
| `solicitudes.creadas.dlq` | `solicitudes.creadas.dlx` | TTL 24 h (auditoría) |
| `solicitudes.estados.dlq` | `solicitudes.estados.dlx` | TTL 24 h |
| `solicitudes.notificaciones.dlq` | `solicitudes.notificaciones.dlx` | TTL 24 h |

Toda la topología se declara en [`config/RabbitMQConfig.java`](src/main/java/cl/duoc/ms_barriodigital_notifications/config/RabbitMQConfig.java) y se crea en RabbitMQ al arrancar el servicio.

---

## Contrato de eventos

Los productores publican en **ambos** exchanges con la misma routing key y `content_type: application/json`.

**`solicitud.created`**
```json
{ "id": 15, "tipoTramite": "Hoyo en la calle", "vecinoEmail": "vecino@correo.cl", "estado": "INGRESADO", "fecha": "2026-10-15T23:32:24" }
```

**`solicitud.status`**
```json
{ "id": 15, "estadoAnterior": "INGRESADO", "estadoNuevo": "ADMITIDO", "fecha": "2026-10-15T23:40:10" }
```

`id` es obligatorio: un evento sin `id` se rechaza y va a la DLQ. Los campos extra se ignoran.

---

## Estructura

```
src/main/java/cl/duoc/ms_barriodigital_notifications/
├── config/
│   └── RabbitMQConfig.java              Topología, conversor JSON, recoverer y factory de las DLQ
├── dto/
│   ├── SolicitudCreadaEvent.java
│   └── SolicitudEstadoEvent.java
└── consumer/
    ├── ConsumidorBase.java              ACK / NACK / validación comunes
    ├── solicitud/
    │   ├── SolicitudCreadaConsumer.java
    │   └── SolicitudEstadoConsumer.java
    ├── notificacion/
    │   └── NotificacionGeneralConsumer.java
    └── dlq/
        └── DlqConsumer.java             Monitor de las 3 DLQ
docker-compose.yml                       Cluster RabbitMQ de 2 nodos
docker/rabbitmq/cluster-entrypoint.sh    Une rabbit2 al cluster de rabbit1
```

---

## Decisiones de diseño

### Configuración centralizada y separada del negocio
Todos los nombres (colas, exchanges, routing keys, DLX y DLQ) son constantes de `RabbitMQConfig`, y ninguna otra clase escribe un nombre a mano. Los consumidores **no declaran** nada: solo escuchan colas que la configuración ya creó.

### Consumidores por dominio funcional
Una clase por cola, agrupadas por dominio (`solicitud`, `notificacion` y `dlq`). La lógica de confirmación vive una sola vez en `ConsumidorBase`.

### ACK manual (`acknowledge-mode=manual`)
Un mensaje se confirma con `basicAck` **solo después** de procesarse bien. Si el servicio se cae a la mitad, el mensaje sigue en RabbitMQ y se vuelve a entregar: no se pierde. Con `prefetch=1`, cada consumidor procesa un mensaje a la vez.

### Dos rutas de error, diferenciadas

| Tipo de error | Ejemplo | Qué pasa |
|---------------|---------|----------|
| **De negocio** (no recuperable) | Evento sin `id` | El consumidor lo atrapa → `basicNack(requeue=false)` → **directo a la DLQ**, sin reintentos, porque volvería a fallar |
| **Técnico** | El body no es un JSON válido y no se puede convertir al DTO | **Reintentos** con backoff exponencial (1 s, 2 s, 4 s). Al agotarse, `rechazarADlqRecoverer` lo rechaza → **DLQ** |
| **Expiración** | Nadie consume el mensaje en 30 s | RabbitMQ lo mueve solo a la DLQ (`x-message-ttl`) |

`requeue=false` es intencional: reencolar un mensaje inválido produciría un ciclo infinito de reprocesos.

### Recoverer propio con ACK manual
En modo manual, el recoverer por defecto de Spring **no hace NACK** al agotar los reintentos, y el mensaje quedaría *unacked* para siempre. `rechazarADlqRecoverer` lanza `AmqpRejectAndDontRequeueException(rejectManual=true)` para que el contenedor lo rechace hacia la DLQ.

### Factory propia para las DLQ
`DlqConsumer` usa `dlqContainerFactory`: sin conversor JSON y sin reintentos. A la DLQ llegan justamente los mensajes mal formados. Si el monitor intentara convertirlos a JSON, fallaría y el mensaje se perdería, porque la DLQ no tiene a su vez otra DLQ. El monitor lee el body como texto, registra el motivo (`x-death`: `rejected` o `expired`) y hace ACK.

### Conversor `JacksonJsonMessageConverter`
Spring Boot 4 usa Jackson 3. `Jackson2JsonMessageConverter` depende de Jackson 2 y fallaría; `JacksonJsonMessageConverter` es su reemplazo en Spring AMQP 4. Convierte el JSON al DTO usando el tipo del parámetro del `@RabbitListener`, sin depender de las clases del productor.

### Cluster de 2 nodos
`rabbit1` y `rabbit2` comparten la Erlang cookie. `cluster-entrypoint.sh` une `rabbit2` a `rabbit1` (`stop_app` → `join_cluster` → `start_app`) y es idempotente: si ya pertenece al cluster, no vuelve a unirse. Los dos nodos arrancan juntos, porque en RabbitMQ 4.x un nodo de un cluster ya formado necesita a la mayoría para terminar de arrancar.

---

## Cómo correrlo

### Requisitos
- Docker Desktop.
- Java 17 o superior.
- Puertos libres: 5672, 5673, 15672, 15673 y 8082.

### 1. Levantar el cluster RabbitMQ
```bash
docker compose up -d
docker ps                                        # rabbit1 y rabbit2: healthy
docker exec rabbit1 rabbitmqctl cluster_status   # Running Nodes: rabbit@rabbit1, rabbit@rabbit2
```
Panel de administración: http://localhost:15672 (usuario `barriodigital`, contraseña `barrio2026Rabbit`). El nodo 2 está en http://localhost:15673.

### 2. Levantar el microservicio
```bash
# Linux / macOS
RABBITMQ_USERNAME=barriodigital RABBITMQ_PASSWORD=barrio2026Rabbit ./mvnw clean spring-boot:run
```
```powershell
# Windows (PowerShell)
$env:RABBITMQ_USERNAME="barriodigital"; $env:RABBITMQ_PASSWORD="barrio2026Rabbit"; .\mvnw.cmd clean spring-boot:run
```

### Variables de entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `RABBITMQ_HOST` | `localhost` | Host del broker |
| `RABBITMQ_PORT` | `5672` | Puerto AMQP |
| `RABBITMQ_USERNAME` | `guest` | Usuario. **En el cluster es `barriodigital`** |
| `RABBITMQ_PASSWORD` | `guest` | Contraseña |

El `docker-compose.yml` acepta `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD` y `RABBITMQ_ERLANG_COOKIE` para sobrescribir las credenciales y la cookie de desarrollo.

---

## Cómo probarlo

Publica un evento desde el panel (*Exchanges → `solicitudes.direct` → Publish message*, con la propiedad `content_type = application/json`) o por consola:

```bash
docker exec rabbit1 rabbitmqctl list_queues name messages consumers
```

| Prueba | Routing key | Body | Resultado esperado en el log |
|--------|-------------|------|------------------------------|
| Evento válido | `solicitud.created` | JSON completo con `id` | `Notificación: nueva solicitud #...` (ACK) |
| Sin `id` | `solicitud.created` | `{"tipoTramite":"x"}` | `Error procesando...` → `Mensaje muerto... Motivo: rejected` |
| JSON inválido | `solicitud.status` | `{no es json` | `Reintentos agotados...` → `Mensaje muerto... Motivo: rejected` |
| TTL | cualquiera, con el servicio detenido más de 30 s | | Al volver: `Mensaje muerto... Motivo: expired` |

Al terminar, todas las colas deben quedar con `messages_ready = 0` y `messages_unacknowledged = 0`:
```bash
docker exec rabbit1 rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

---

## Problemas comunes

| Síntoma | Solución |
|---------|----------|
| `ACCESS_REFUSED` al arrancar | Faltan `RABBITMQ_USERNAME` y `RABBITMQ_PASSWORD`: los defaults `guest/guest` no sirven contra el cluster |
| Un nodo queda en `health: starting` | Se levantó solo uno; `docker compose up -d` levanta los dos |
| `rabbit2` no se une | `docker compose down -v` y `docker compose up -d` (borra los datos del cluster) |
| Clases mal compiladas por el IDE | Usa siempre `clean` en el comando de Maven |
