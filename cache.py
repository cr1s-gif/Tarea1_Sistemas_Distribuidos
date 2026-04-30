import json
import os
import time
import requests

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

URL_GENERADOR_RESPUESTAS = os.getenv(
    "URL_GENERADOR_RESPUESTAS",
    "http://localhost:8001/resolver"
)

cliente_redis = None


# -----------------------------
# Inicio del servicio
# -----------------------------
@app.on_event("startup")
def iniciar_servicio():
    global cliente_redis

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


def extraer_datos_metricas(consulta):
    tipo_consulta = consulta.get("tipo_consulta")
    zona_id = consulta.get("zona_id")
    parametros = consulta.get("parametros", {})

    zona_a = parametros.get("zona_a")
    zona_b = parametros.get("zona_b")

    return tipo_consulta, zona_id, zona_a, zona_b


def pedir_respuesta_al_generador(consulta):
    respuesta_http = requests.post(URL_GENERADOR_RESPUESTAS, json=consulta, timeout=30)
    respuesta_http.raise_for_status()
    return respuesta_http.json()


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    redis_ok = probar_conexion(cliente_redis) if cliente_redis else False

    return {
        "servicio": "cache",
        "redis_conectado": redis_ok
    }


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

    try:
        respuesta_generada = pedir_respuesta_al_generador(consulta)
    except Exception as e:
        return {
            "error": f"No se pudo obtener respuesta del generador de respuestas: {str(e)}"
        }

    guardar_en_cache(clave_cache, respuesta_generada, TTL_SEGUNDOS)

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
        origen_respuesta="generador_respuestas"
    )

    return {
        "cache_hit": False,
        "origen": "generador_respuestas",
        "clave_cache": clave_cache,
        "respuesta": respuesta_generada
    }

