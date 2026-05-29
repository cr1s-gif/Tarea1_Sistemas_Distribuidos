import json
import os
import signal
import time

import redis
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from generador_respuestas import (
    cargar_dataset,
    responder_consulta,
    separar_datos_por_zona,
)
from metricas import inicializar_archivo_metricas, registrar_metrica


HOST_REDIS = os.getenv("HOST_REDIS", "redis")
PUERTO_REDIS = int(os.getenv("PUERTO_REDIS", "6379"))
DB_REDIS = int(os.getenv("DB_REDIS", "0"))
TTL_SEGUNDOS = int(os.getenv("TTL_SEGUNDOS", "120"))
TTL_ESTADO_CONSULTA_SEGUNDOS = int(os.getenv("TTL_ESTADO_CONSULTA_SEGUNDOS", "600"))

RUTA_CSV = os.getenv("RUTA_CSV", "967_buildings.csv")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_CONSULTAS = os.getenv("TOPIC_CONSULTAS", "consultas")
TOPIC_RESPUESTAS = os.getenv("TOPIC_RESPUESTAS", "respuestas")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "consultas_retry")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas_dlq")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer-respuestas")
MAX_INTENTOS = int(os.getenv("MAX_INTENTOS", "3"))
ESCENARIO = os.getenv("ESCENARIO", "kafka")
NUM_CONSUMIDORES = int(os.getenv("NUM_CONSUMIDORES", "1"))

ejecutando = True


def manejar_salida(signum, frame):
    global ejecutando
    ejecutando = False


def crear_cliente_redis():
    cliente = redis.Redis(
        host=HOST_REDIS,
        port=PUERTO_REDIS,
        db=DB_REDIS,
        decode_responses=True,
    )
    cliente.ping()
    return cliente


def construir_clave_estado_consulta(consulta_id):
    return f"consulta_estado:{consulta_id}"


def guardar_estado_consulta(cliente_redis, consulta_id, estado):
    cliente_redis.setex(
        construir_clave_estado_consulta(consulta_id),
        TTL_ESTADO_CONSULTA_SEGUNDOS,
        json.dumps(estado),
    )


def guardar_respuesta_cache(cliente_redis, clave_cache, respuesta):
    cliente_redis.setex(clave_cache, TTL_SEGUNDOS, json.dumps(respuesta))


def extraer_datos_metricas(consulta):
    tipo_consulta = consulta.get("tipo_consulta")
    zona_id = consulta.get("zona_id")
    parametros = consulta.get("parametros", {})

    return (
        tipo_consulta,
        zona_id,
        parametros.get("zona_a"),
        parametros.get("zona_b"),
    )


def crear_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_CONSULTAS, TOPIC_RETRY])
    return consumer


def asegurar_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics = [
        NewTopic(TOPIC_CONSULTAS, num_partitions=1, replication_factor=1),
        NewTopic(TOPIC_RETRY, num_partitions=1, replication_factor=1),
        NewTopic(TOPIC_RESPUESTAS, num_partitions=1, replication_factor=1),
        NewTopic(TOPIC_DLQ, num_partitions=1, replication_factor=1),
    ]

    resultados = admin.create_topics(topics)
    for topic, future in resultados.items():
        try:
            future.result()
            print(f"Topic creado: {topic}")
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                print(f"No se pudo crear topic {topic}: {exc}")


def crear_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "consumer-respuestas",
    })


def publicar_mensaje(producer, topic, clave_cache, payload):
    errores_entrega = []

    def confirmar_entrega(error, mensaje):
        if error is not None:
            errores_entrega.append(str(error))

    producer.produce(
        topic,
        key=clave_cache,
        value=json.dumps(payload),
        callback=confirmar_entrega,
    )
    mensajes_pendientes = producer.flush(5)

    if errores_entrega:
        raise KafkaException(errores_entrega[0])

    if mensajes_pendientes > 0:
        raise TimeoutError(f"Kafka no confirmo la publicacion en {topic}")


def publicar_respuesta(producer, clave_cache, resultado):
    publicar_mensaje(producer, TOPIC_RESPUESTAS, clave_cache, resultado)


def publicar_retry(producer, clave_cache, payload):
    publicar_mensaje(producer, TOPIC_RETRY, clave_cache, payload)


def publicar_dlq(producer, clave_cache, payload):
    publicar_mensaje(producer, TOPIC_DLQ, clave_cache, payload)


def calcular_backlog_size(consumer):
    try:
        backlog_total = 0
        asignaciones = consumer.assignment()

        if not asignaciones:
            return 0

        offsets_confirmados = consumer.committed(asignaciones, timeout=5)

        for particion in offsets_confirmados:
            if particion.offset < 0:
                continue

            low, high = consumer.get_watermark_offsets(particion, timeout=5)
            backlog_total += max(high - particion.offset, 0)

        return backlog_total
    except Exception as exc:
        print(f"No se pudo calcular backlog: {exc}")
        return None


def procesar_mensaje(mensaje, datos_por_zona, areas_por_zona, cliente_redis, producer, backlog_size):
    inicio = time.time()
    payload = json.loads(mensaje.value().decode("utf-8"))

    consulta_id = payload["consulta_id"]
    clave_cache = payload["clave_cache"]
    consulta = payload["consulta"]
    intentos = int(payload.get("intentos", 0))
    topic_origen = mensaje.topic()

    respuesta = responder_consulta(consulta, datos_por_zona, areas_por_zona)
    if isinstance(respuesta, dict) and "error" in respuesta:
        raise RuntimeError(respuesta["error"])

    guardar_respuesta_cache(cliente_redis, clave_cache, respuesta)

    resultado = {
        "consulta_id": consulta_id,
        "estado": "completada",
        "cache_hit": False,
        "origen": "consumer_respuestas",
        "topic_origen": topic_origen,
        "topic_respuesta": TOPIC_RESPUESTAS,
        "clave_cache": clave_cache,
        "consulta": consulta,
        "intentos": intentos,
        "respuesta": respuesta,
        "backlog_size": backlog_size,
        "latencia_procesamiento_ms": (time.time() - inicio) * 1000,
        "timestamp_completada": time.time(),
    }

    guardar_estado_consulta(cliente_redis, consulta_id, resultado)
    publicar_respuesta(producer, clave_cache, resultado)

    tipo_consulta, zona_id, zona_a, zona_b = extraer_datos_metricas(consulta)
    recovery_time_ms = None
    if payload.get("timestamp_creacion") is not None:
        recovery_time_ms = (time.time() - float(payload["timestamp_creacion"])) * 1000

    registrar_metrica(
        tipo_consulta=tipo_consulta,
        zona_id=zona_id,
        zona_a=zona_a,
        zona_b=zona_b,
        cache_hit=False,
        latencia_ms=resultado["latencia_procesamiento_ms"],
        throughput_qps=0.0,
        ttl_segundos=TTL_SEGUNDOS,
        clave_cache=clave_cache,
        origen_respuesta="consumer_respuestas",
        consulta_id=consulta_id,
        estado="completada",
        intentos=intentos,
        max_intentos=MAX_INTENTOS,
        topic_origen=topic_origen,
        topic_respuesta=TOPIC_RESPUESTAS,
        retry_count=intentos,
        dlq=False,
        recuperada=intentos > 0,
        backlog_size=backlog_size,
        recovery_time_ms=recovery_time_ms,
        escenario=ESCENARIO,
        num_consumidores=NUM_CONSUMIDORES,
    )

    return resultado


def registrar_evento_fallo(payload, topic_origen, topic_destino, estado, exc, backlog_size, dlq=False):
    consulta = payload.get("consulta", {})
    tipo_consulta, zona_id, zona_a, zona_b = extraer_datos_metricas(consulta)
    intentos = int(estado.get("intentos", 0))

    registrar_metrica(
        tipo_consulta=tipo_consulta,
        zona_id=zona_id,
        zona_a=zona_a,
        zona_b=zona_b,
        cache_hit=False,
        latencia_ms=0.0,
        throughput_qps=0.0,
        ttl_segundos=TTL_SEGUNDOS,
        clave_cache=payload.get("clave_cache"),
        origen_respuesta="consumer_respuestas",
        consulta_id=payload.get("consulta_id"),
        estado="dlq" if dlq else "retry",
        intentos=intentos,
        max_intentos=MAX_INTENTOS,
        topic_origen=topic_origen,
        topic_destino=topic_destino,
        retry_count=intentos,
        dlq=dlq,
        recuperada=False,
        backlog_size=backlog_size,
        escenario=ESCENARIO,
        num_consumidores=NUM_CONSUMIDORES,
        error=str(exc),
    )


def manejar_error_procesamiento(mensaje, exc, cliente_redis, producer, backlog_size):
    payload = json.loads(mensaje.value().decode("utf-8"))
    consulta_id = payload["consulta_id"]
    clave_cache = payload["clave_cache"]
    intentos = int(payload.get("intentos", 0)) + 1

    payload_error = {
        **payload,
        "intentos": intentos,
        "ultimo_error": str(exc),
        "ultimo_topic": mensaje.topic(),
        "backlog_size": backlog_size,
        "timestamp_ultimo_error": time.time(),
    }

    if intentos >= MAX_INTENTOS:
        estado = {
            **payload_error,
            "estado": "dlq",
            "topic_destino": TOPIC_DLQ,
            "max_intentos": MAX_INTENTOS,
        }
        guardar_estado_consulta(cliente_redis, consulta_id, estado)
        publicar_dlq(producer, clave_cache, estado)
        registrar_evento_fallo(payload, mensaje.topic(), TOPIC_DLQ, estado, exc, backlog_size, dlq=True)
        return estado

    estado = {
        **payload_error,
        "estado": "retry",
        "topic_destino": TOPIC_RETRY,
        "max_intentos": MAX_INTENTOS,
    }
    guardar_estado_consulta(cliente_redis, consulta_id, estado)
    publicar_retry(producer, clave_cache, estado)
    registrar_evento_fallo(payload, mensaje.topic(), TOPIC_RETRY, estado, exc, backlog_size, dlq=False)
    return estado


def main():
    signal.signal(signal.SIGINT, manejar_salida)
    signal.signal(signal.SIGTERM, manejar_salida)

    inicializar_archivo_metricas()

    print("Cargando dataset para consumer_respuestas...")
    df = cargar_dataset(RUTA_CSV)
    datos_por_zona, areas_por_zona = separar_datos_por_zona(df)
    print("Dataset cargado correctamente.")

    print("Conectando a Redis...")
    cliente_redis = crear_cliente_redis()
    print("Conexion con Redis exitosa.")

    print(f"Conectando a Kafka en {KAFKA_BOOTSTRAP_SERVERS}...")
    asegurar_topics()
    consumer = crear_consumer()
    producer = crear_producer()
    print(f"Escuchando topics: {TOPIC_CONSULTAS}, {TOPIC_RETRY}")

    try:
        while ejecutando:
            mensaje = consumer.poll(1.0)

            if mensaje is None:
                continue

            if mensaje.error():
                print(f"Error Kafka: {mensaje.error()}")
                continue

            backlog_size = calcular_backlog_size(consumer)

            try:
                resultado = procesar_mensaje(
                    mensaje,
                    datos_por_zona,
                    areas_por_zona,
                    cliente_redis,
                    producer,
                    backlog_size,
                )
                consumer.commit(mensaje)
                print(
                    "Consulta procesada:",
                    resultado["consulta_id"],
                    resultado["clave_cache"],
                    f"backlog={backlog_size}",
                )
            except Exception as exc:
                try:
                    estado = manejar_error_procesamiento(
                        mensaje,
                        exc,
                        cliente_redis,
                        producer,
                        backlog_size,
                    )
                    consumer.commit(mensaje)
                    print(
                        "Consulta enviada a",
                        estado["estado"],
                        estado["consulta_id"],
                        estado["clave_cache"],
                        f"intentos={estado['intentos']}",
                        f"backlog={backlog_size}",
                    )
                except Exception as error_retry:
                    print(f"Error procesando retry/DLQ: {error_retry}")
    finally:
        consumer.close()
        producer.flush(5)


if __name__ == "__main__":
    main()
