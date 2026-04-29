# Tarea 1 — Sistemas Distribuidos: Cache con Redis

Sistema distribuido que implementa una capa de caché sobre un generador de respuestas geoespaciales, usando datos de edificios de la Región Metropolitana de Chile.

---

## Descripción general

El sistema está compuesto por **4 contenedores Docker** que trabajan en conjunto:

| Contenedor | Puerto | Rol |
|---|---|---|
| `redis` | 6379 | Almacén de caché con política LRU  |
| `generador_respuestas` | 8001 | Carga el CSV y resuelve consultas geoespaciales |
| `cache` | 8000 | Intermediario: sirve desde Redis o delega al generador |
| `generador_trafico` | — | Genera consultas automáticas hacia el caché |

### Flujo de una consulta

```
generador_trafico
      │
      ▼
   cache:8000  ──── Redis HIT ──▶ respuesta inmediata
      │
      └── Redis MISS ──▶ generador_respuestas:8001 ──▶ guarda en Redis ──▶ respuesta
```

---

## Tipos de consulta soportados

Todas las consultas apuntan a una de las 5 zonas predefinidas de Santiago:

| ID | Zona |
|---|---|
| Z1 | Providencia |
| Z2 | Las Condes |
| Z3 | Maipú |
| Z4 | Santiago Centro |
| Z5 | Pudahuel |

| Tipo | Descripción |
|---|---|
| **Q1** | Cantidad de edificios en una zona con confianza mínima |
| **Q2** | Área promedio y total de edificios en una zona |
| **Q3** | Densidad de edificios por km² en una zona |
| **Q4** | Comparación de densidad entre dos zonas |
| **Q5** | Distribución de confianza en histograma (bins configurables) |

---

## Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y en ejecución
- El archivo CSV del dataset (ver sección siguiente)

---

## Dataset: descarga del CSV

El archivo `967_buildings.csv` contiene los datos de edificios de la Región Metropolitana y **no está incluido en el repositorio** por su tamaño.

**Pasos para descargarlo:**

1. Ir a [https://sites.research.google/gr/open-buildings/](https://sites.research.google/gr/open-buildings/)
2. Seleccionar la región de Chile / Región Metropolitana
3. Descargar el archivo `967_buildings.csv`
4. **Colocarlo en la raíz del repositorio** (junto a los demás archivos `.py` y `docker-compose.yml`)

> El archivo debe llamarse exactamente `967_buildings.csv`. El servicio `generador_respuestas` lo busca con ese nombre dentro del contenedor.

---

## Estructura del repositorio

```
Tarea1_Sistemas_Distribuidos/
├── cache.py                    # Servicio de caché (FastAPI, puerto 8000)
├── generador_respuestas.py     # Servicio de resolución de consultas (FastAPI, puerto 8001)
├── generador_trafico.py        # Generador de consultas automáticas
├── conexion_redis.py           # Utilidades para conexión con Redis
├── metricas.py                 # Registro y cálculo de métricas
├── Dockerfile.cache            # Imagen del servicio cache
├── Dockerfile.respuestas       # Imagen del servicio generador_respuestas
├── Dockerfile.trafico          # Imagen del servicio generador_trafico
├── docker-compose.yml          # Orquestación de todos los servicios
├── requirements.txt            # Dependencias Python
├── metricas_output/
│   └── metricas.csv            # Métricas generadas en ejecución (montado como volumen)
└── 967_buildings.csv           # ⚠️ No incluido — descargar por separado
```

---

## Levantar el sistema

### Windows (Docker Desktop)

```bash
docker compose up --build
```

### macOS / Linux

```bash
docker compose up --build
```

> El comando es idéntico en los tres sistemas operativos. Docker Desktop en Windows usa el backend WSL2, por lo que no requiere ningún ajuste adicional.

**Primera ejecución:** Docker descargará la imagen base de Python 3.11 e instalará las dependencias. Esto puede tardar unos minutos. Las ejecuciones siguientes serán más rápidas gracias al caché de capas de Docker.

### Para ejecutar en segundo plano

```bash
docker compose up --build -d
```

### Para detener

```bash
docker compose down
```

---

## Verificar que todo funciona

Una vez levantados los contenedores, podés verificar el estado de los servicios:

```bash
# Estado del servicio de caché
curl http://localhost:8000/health

# Estado del generador de respuestas
curl http://localhost:8001/health
```

Respuesta esperada del caché:
```json
{"servicio": "cache", "redis_conectado": true}
```

---

## Hacer una consulta manual

También podés enviar consultas directamente al servicio de caché:

**Q1 — Contar edificios en Providencia con confianza ≥ 0.5:**
```bash
curl -X POST http://localhost:8000/consulta \
  -H "Content-Type: application/json" \
  -d '{"tipo_consulta": "Q1", "zona_id": "Z1", "parametros": {"confianza_min": 0.5}}'
```

**Q4 — Comparar densidad entre dos zonas:**
```bash
curl -X POST http://localhost:8000/consulta \
  -H "Content-Type: application/json" \
  -d '{"tipo_consulta": "Q4", "parametros": {"zona_a": "Z1", "zona_b": "Z2", "confianza_min": 0.3}}'
```

La respuesta incluirá `"cache_hit": true` o `"cache_hit": false` según si la respuesta fue servida desde Redis o calculada en el momento.

---

## Métricas

El sistema registra automáticamente métricas de cada consulta en `metricas_output/metricas.csv`. Este archivo se genera en la máquina host (gracias al volumen Docker) y persiste entre reinicios del contenedor.

Columnas registradas: `timestamp`, `tipo_consulta`, `zona_id`, `cache_hit`, `latencia_ms`, `throughput_qps`, `ttl_segundos`, `clave_cache`, `origen_respuesta`, entre otras.

---

## Variables de entorno configurables

El comportamiento de los servicios se puede ajustar desde `docker-compose.yml`:

| Variable | Servicio | Default | Descripción |
|---|---|---|---|
| `TTL_SEGUNDOS` | cache | `120` | Tiempo de vida de entradas en Redis |
| `DISTRIBUCION` | cache | `uniforme` | Distribución de consultas: `uniforme` o `zipf` |
| `INTERVALO_SEGUNDOS` | trafico / cache | `10` | Segundos entre consultas automáticas |
| `HOST_REDIS` | cache | `redis` | Hostname del contenedor Redis |
| `RUTA_CSV` | generador_respuestas | `967_buildings.csv` | Nombre del archivo CSV |

---

## Dependencias

```
fastapi
uvicorn
requests
redis
pandas
```

Python 3.11 — instaladas automáticamente dentro de los contenedores al hacer `docker compose up --build`.
