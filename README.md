# Proyecto Sistemas Distribuidos: Cache con Redis y Kafka

Sistema distribuido para resolver consultas geoespaciales sobre edificios de la Region Metropolitana de Chile. El proyecto combina una capa de cache en Redis con procesamiento asincrono mediante Kafka, reintentos, cola de mensajes fallidos y metricas de ejecucion.

## Descripcion General

El sistema esta compuesto por servicios Docker que trabajan en conjunto:

| Servicio | Puerto | Rol |
|---|---:|---|
| `redis` | 6379 | Almacen de cache con politica LRU |
| `kafka` | 9092 | Broker de mensajeria para consultas asincronas |
| `cache` | 8000 | API principal: responde desde Redis o publica la consulta en Kafka |
| `consumer_respuestas` | - | Consume consultas desde Kafka, calcula respuestas, guarda en Redis y maneja retry/DLQ |
| `generador_respuestas` | 8001 | Servicio HTTP original para resolver consultas geoespaciales |
| `generador_trafico` | - | Genera trafico automatico hacia el cache |

## Flujo de Consulta

```text
generador_trafico o cliente manual
        |
        v
cache:8000
        |
        +-- Redis HIT --> respuesta inmediata
        |
        +-- Redis MISS --> Kafka topic: consultas
                              |
                              v
                       consumer_respuestas
                              |
                              +-- exito --> Redis + topic respuestas + estado completada
                              |
                              +-- error recuperable --> topic consultas_retry
                              |
                              +-- max intentos --> topic consultas_dlq
```

Cuando una consulta no esta en cache, el endpoint `POST /consulta` devuelve un `consulta_id` con estado `pendiente`. El resultado final se puede consultar con `GET /consulta/{consulta_id}`.

## Tipos de Consulta Soportados

Todas las consultas usan una de las 5 zonas predefinidas de Santiago:

| ID | Zona |
|---|---|
| `Z1` | Providencia |
| `Z2` | Las Condes |
| `Z3` | Maipu |
| `Z4` | Santiago Centro |
| `Z5` | Pudahuel |

| Tipo | Descripcion |
|---|---|
| `Q1` | Cantidad de edificios en una zona con confianza minima |
| `Q2` | Area promedio y total de edificios en una zona |
| `Q3` | Densidad de edificios por km2 en una zona |
| `Q4` | Comparacion de densidad entre dos zonas |
| `Q5` | Distribucion de confianza en histograma |

## Requisitos

- Docker Desktop instalado y en ejecucion.
- Archivo `967_buildings.csv` descargado y ubicado en la raiz del proyecto.

## Dataset

El archivo `967_buildings.csv` no esta incluido en el repositorio por su tamano.

Pasos:

1. Ir a `https://sites.research.google/gr/open-buildings/`.
2. Descargar el archivo correspondiente a Chile / Region Metropolitana.
3. Renombrarlo exactamente como `967_buildings.csv` si fuera necesario.
4. Colocarlo en la raiz del proyecto, junto a `docker-compose.yml`.

El CSV se monta como volumen dentro de los contenedores que lo necesitan. No se copia dentro de la imagen Docker para evitar builds muy lentos.

## Estructura del Proyecto

```text
Proyecto_Sistemas_Distribuidos/
|-- cache.py
|-- conexion_redis.py
|-- consumer_respuestas.py
|-- generador_respuestas.py
|-- generador_trafico.py
|-- metricas.py
|-- docker-compose.yml
|-- Dockerfile.cache
|-- Dockerfile.consumer
|-- Dockerfile.respuestas
|-- Dockerfile.trafico
|-- requirements.txt
|-- .dockerignore
|-- metricas_output/
|   `-- metricas.csv
`-- 967_buildings.csv
```

## Levantar el Sistema

```bash
docker compose up --build -d
```

Para ver el estado:

```bash
docker compose ps
```

Para detener todo:

```bash
docker compose down
```

## Verificar Servicios

Estado del cache:

```bash
curl http://localhost:8000/health
```

Respuesta esperada:

```json
{
  "servicio": "cache",
  "redis_conectado": true,
  "kafka_configurado": true,
  "kafka_bootstrap_servers": "kafka:29092",
  "topic_consultas": "consultas"
}
```

Estado del generador HTTP original:

```bash
curl http://localhost:8001/health
```

## Consultas Manuales

### Q1

```bash
curl -X POST http://localhost:8000/consulta \
  -H "Content-Type: application/json" \
  -d '{"tipo_consulta":"Q1","zona_id":"Z1","parametros":{"confianza_min":0.5}}'
```

Si no existe en cache, la respuesta sera parecida a:

```json
{
  "cache_hit": false,
  "origen": "kafka",
  "estado": "pendiente",
  "consulta_id": "uuid",
  "topic": "consultas",
  "clave_cache": "count:Z1:conf=0.5"
}
```

Luego se consulta el estado:

```bash
curl http://localhost:8000/consulta/uuid
```

Cuando el consumidor termina:

```json
{
  "estado": "completada",
  "origen": "consumer_respuestas",
  "respuesta": {
    "tipo_consulta": "Q1",
    "zona_id": "Z1",
    "cantidad_edificios": 15639
  }
}
```

Si se repite la misma consulta dentro del TTL, responde desde Redis:

```json
{
  "cache_hit": true,
  "origen": "cache"
}
```

### Q4

```bash
curl -X POST http://localhost:8000/consulta \
  -H "Content-Type: application/json" \
  -d '{"tipo_consulta":"Q4","parametros":{"zona_a":"Z1","zona_b":"Z2","confianza_min":0.5}}'
```

## Trafico Automatico

El servicio `generador_trafico` envia consultas automaticamente al cache. Si el contenedor esta corriendo, se generara trafico continuo segun las variables configuradas en `docker-compose.yml`.

Iniciar solo el generador de trafico:

```bash
docker compose start generador_trafico
```

Detenerlo:

```bash
docker compose stop generador_trafico
```

Con carga normal es posible que el consumidor procese rapido y el lag de Kafka se mantenga en `0`. Para forzar acumulacion de cola se puede ajustar:

| Variable | Descripcion |
|---|---|
| `MODO_TRAFICO` | `normal`, `alta_carga` o `spike` |
| `CONSULTAS_POR_SEGUNDO` | QPS fijo; si es `0`, usa el modo configurado |
| `SPIKE_CONSULTAS_POR_SEGUNDO` | QPS durante un spike |
| `DURACION_SPIKE_SEGUNDOS` | Duracion del spike |
| `CONCURRENCIA` | Maximo de workers concurrentes |
| `DISTRIBUCION` | `uniforme` o `zipf` |

## Kafka, Retry y DLQ

Topicos usados:

| Topico | Uso |
|---|---|
| `consultas` | Consultas nuevas publicadas por el cache |
| `respuestas` | Resultados completados |
| `consultas_retry` | Consultas que fallaron y deben reintentarse |
| `consultas_dlq` | Consultas fallidas despues del maximo de intentos |

El consumidor usa `MAX_INTENTOS=3`. Si una consulta falla 3 veces, queda con estado `dlq`.

Ver lag/backlog del grupo consumidor:

```bash
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group consumer-respuestas
```

## Metricas

Las metricas se guardan en:

```text
metricas_output/metricas.csv
```

Incluyen datos como:

- `cache_hit`
- `latencia_ms`
- `throughput_qps`
- `consulta_id`
- `estado`
- `intentos`
- `retry_count`
- `dlq`
- `backlog_size`
- `recovery_time_ms`
- `escenario`
- `num_consumidores`

## Variables Principales

| Variable | Servicio | Descripcion |
|---|---|---|
| `TTL_SEGUNDOS` | cache/consumer | Tiempo de vida de entradas en Redis |
| `TTL_ESTADO_CONSULTA_SEGUNDOS` | cache/consumer | Tiempo de vida del estado de una consulta |
| `KAFKA_BOOTSTRAP_SERVERS` | cache/consumer | Broker Kafka |
| `TOPIC_CONSULTAS` | cache/consumer | Topico de consultas |
| `TOPIC_RESPUESTAS` | consumer | Topico de respuestas |
| `TOPIC_RETRY` | consumer | Topico de reintentos |
| `TOPIC_DLQ` | consumer | Topico de mensajes fallidos |
| `MAX_INTENTOS` | consumer | Maximo de intentos antes de DLQ |
| `SIMULAR_FALLOS` | generador/consumer | Activa fallos artificiales |
| `PROBABILIDAD_FALLO` | generador/consumer | Probabilidad de fallo artificial |
| `LATENCIA_ARTIFICIAL_MS` | generador/consumer | Latencia artificial por consulta |

## Dependencias

```text
fastapi
uvicorn
requests
redis
pandas
confluent-kafka
```

Python 3.11 se usa dentro de los contenedores Docker.
