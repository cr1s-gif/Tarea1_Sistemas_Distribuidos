import json
import os
import time
import uuid

from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI
from conexion_redis import crear_cliente_redis, probar_conexion
from metricas import inicializar_archivo_metricas, registrar_metrica

app = FastAPI()

# -----------------------------
# Configuracion
# -----------------------------
HOST_REDIS = os.getenv("HOST_REDIS", "localhost")
PUERTO_REDIS = int(os.getenv("PUERTO_REDIS", "6379"))
DB_REDIS = int(os.getenv("DB_REDIS", "0"))

TTL_SEGUNDOS = int(os.getenv("TTL_SEGUNDOS", "120"))
TTL_ESTADO_CONSULTA_SEGUNDOS = int(os.getenv("TTL_ESTADO_CONSULTA_SEGUNDOS", "600"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_CONSULTAS = os.getenv("TOPIC_CONSULTAS", "consultas")

cliente_redis = None
productor_kafka = None


# -----------------------------
# Inicio del servicio
# -----------------------------
@app.on_event("startup")
def iniciar_servicio():
    global cliente_redis, productor_kafka

    print("Inicializando archivo de métricas...")
    inicializar_archivo_metricas()

    print("Conectando a Redis...")
    cliente_redis = crear_cliente_redis(
        host=HOST_REDIS,
        puerto=PUERTO_REDIS,
        db=DB_REDIS
    )

    if not probar_conexion(cliente_redis):
        raise Exception("No se pudo conectar a Redis")

    print("Conexión con Redis exitosa.")

    print("Configurando productor Kafka...")
    productor_kafka = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "cache"
    })
    print("Productor Kafka configurado.")


# -----------------------------
# Utilidades
# -----------------------------
def construir_clave_cache(consulta):
    tipo_consulta = consulta.get("tipo_consulta")
    parametros = consulta.get("parametros", {})

    if tipo_consulta == "Q1":
        zona_id = consulta.get("zona_id")
        confianza_min = parametros.get("confianza_min", 0.0)
        return f"count:{zona_id}:conf={confianza_min}"

    elif tipo_consulta == "Q2":
        zona_id = consulta.get("zona_id")
        confianza_min = parametros.get("confianza_min", 0.0)
        return f"area:{zona_id}:conf={confianza_min}"

    elif tipo_consulta == "Q3":
        zona_id = consulta.get("zona_id")
        confianza_min = parametros.get("confianza_min", 0.0)
        return f"density:{zona_id}:conf={confianza_min}"

    elif tipo_consulta == "Q4":
        zona_a = parametros.get("zona_a")
        zona_b = parametros.get("zona_b")
        confianza_min = parametros.get("confianza_min", 0.0)

        zonas_ordenadas = sorted([zona_a, zona_b])
        return f"compare:density:{zonas_ordenadas[0]}:{zonas_ordenadas[1]}:conf={confianza_min}"

    elif tipo_consulta == "Q5":
        zona_id = consulta.get("zona_id")
        bins = parametros.get("bins", 5)
        return f"confidence_dist:{zona_id}:bins={bins}"

    return None


def buscar_en_cache(clave):
    valor = cliente_redis.get(clave)

    if valor is None:
        return None

    return json.loads(valor)


def guardar_en_cache(clave, respuesta, ttl_segundos=120):
    respuesta_json = json.dumps(respuesta)
    cliente_redis.setex(clave, ttl_segundos, respuesta_json)


def construir_clave_estado_consulta(consulta_id):
    return f"consulta_estado:{consulta_id}"


def guardar_estado_consulta(consulta_id, estado):
    cliente_redis.setex(
        construir_clave_estado_consulta(consulta_id),
        TTL_ESTADO_CONSULTA_SEGUNDOS,
        json.dumps(estado)
    )


def buscar_estado_consulta(consulta_id):
    valor = cliente_redis.get(construir_clave_estado_consulta(consulta_id))

    if valor is None:
        return None

    return json.loads(valor)


def extraer_datos_metricas(consulta):
    tipo_consulta = consulta.get("tipo_consulta")
    zona_id = consulta.get("zona_id")
    parametros = consulta.get("parametros", {})

    zona_a = parametros.get("zona_a")
    zona_b = parametros.get("zona_b")

    return tipo_consulta, zona_id, zona_a, zona_b


def publicar_consulta_kafka(consulta_id, clave_cache, consulta):
    if productor_kafka is None:
        raise RuntimeError("Productor Kafka no inicializado")

    errores_entrega = []

    def confirmar_entrega(error, mensaje):
        if error is not None:
            errores_entrega.append(str(error))

    mensaje = {
        "consulta_id": consulta_id,
        "clave_cache": clave_cache,
        "consulta": consulta,
        "intentos": 0,
        "timestamp_creacion": time.time()
    }

    productor_kafka.produce(
        TOPIC_CONSULTAS,
        key=clave_cache,
        value=json.dumps(mensaje),
        callback=confirmar_entrega
    )
    mensajes_pendientes = productor_kafka.flush(5)

    if errores_entrega:
        raise KafkaException(errores_entrega[0])

    if mensajes_pendientes > 0:
        raise TimeoutError("Kafka no confirmo la publicacion dentro del timeout")


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    redis_ok = probar_conexion(cliente_redis) if cliente_redis else False

    return {
        "servicio": "cache",
        "redis_conectado": redis_ok,
        "kafka_configurado": productor_kafka is not None,
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic_consultas": TOPIC_CONSULTAS
    }


@app.get("/consulta/{consulta_id}")
def obtener_estado_consulta(consulta_id: str):
    estado = buscar_estado_consulta(consulta_id)

    if estado is None:
        return {
            "consulta_id": consulta_id,
            "estado": "no_encontrada"
        }

    return estado


@app.post("/consulta")
def procesar_consulta_cache(consulta: dict):
    inicio = time.time()

    clave_cache = construir_clave_cache(consulta)

    if clave_cache is None:
        return {"error": "No se pudo construir la clave de caché"}

    tipo_consulta, zona_id, zona_a, zona_b = extraer_datos_metricas(consulta)

    respuesta_cache = buscar_en_cache(clave_cache)

    if respuesta_cache is not None:
        fin = time.time()
        latencia_ms = (fin - inicio) * 1000
        throughput_qps = 1 / (fin - inicio) if (fin - inicio) > 0 else 0.0

        registrar_metrica(
            tipo_consulta=tipo_consulta,
            zona_id=zona_id,
            zona_a=zona_a,
            zona_b=zona_b,
            cache_hit=True,
            latencia_ms=latencia_ms,
            throughput_qps=throughput_qps,
            hubo_eviction=False,
            ttl_segundos=TTL_SEGUNDOS,
            clave_cache=clave_cache,
            origen_respuesta="cache"
        )

        return {
            "cache_hit": True,
            "origen": "cache",
            "clave_cache": clave_cache,
            "respuesta": respuesta_cache
        }

    consulta_id = str(uuid.uuid4())

    estado_pendiente = {
        "consulta_id": consulta_id,
        "estado": "pendiente",
        "cache_hit": False,
        "origen": "kafka",
        "topic": TOPIC_CONSULTAS,
        "clave_cache": clave_cache,
        "consulta": consulta,
        "timestamp_creacion": time.time()
    }
    guardar_estado_consulta(consulta_id, estado_pendiente)

    try:
        publicar_consulta_kafka(consulta_id, clave_cache, consulta)
    except Exception as e:
        estado_error = {
            **estado_pendiente,
            "estado": "error_publicacion",
            "error": str(e),
            "timestamp_error": time.time()
        }
        guardar_estado_consulta(consulta_id, estado_error)

        return {
            "error": f"No se pudo publicar la consulta en Kafka: {str(e)}",
            "consulta_id": consulta_id,
            "clave_cache": clave_cache
        }

    fin = time.time()
    latencia_ms = (fin - inicio) * 1000
    throughput_qps = 1 / (fin - inicio) if (fin - inicio) > 0 else 0.0

    registrar_metrica(
        tipo_consulta=tipo_consulta,
        zona_id=zona_id,
        zona_a=zona_a,
        zona_b=zona_b,
        cache_hit=False,
        latencia_ms=latencia_ms,
        throughput_qps=throughput_qps,
        hubo_eviction=False,
        ttl_segundos=TTL_SEGUNDOS,
        clave_cache=clave_cache,
        origen_respuesta="kafka_pendiente",
        consulta_id=consulta_id,
        estado="pendiente",
        intentos=0,
        retry_count=0,
        dlq=False,
        recuperada=False,
        topic_destino=TOPIC_CONSULTAS
    )

    return {
        "cache_hit": False,
        "origen": "kafka",
        "estado": "pendiente",
        "consulta_id": consulta_id,
        "topic": TOPIC_CONSULTAS,
        "clave_cache": clave_cache,
        "mensaje": "Consulta publicada en Kafka para procesamiento asincrono"
    }

