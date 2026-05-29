import csv
import os
import time
from statistics import median


RUTA_ARCHIVO_METRICAS = "metricas_output/metricas.csv"

COLUMNAS_METRICAS = [
    "timestamp",
    "tipo_consulta",
    "zona_id",
    "zona_a",
    "zona_b",
    "cache_hit",
    "latencia_ms",
    "throughput_qps",
    "hubo_eviction",
    "ttl_segundos",
    "clave_cache",
    "origen_respuesta",
    "consulta_id",
    "estado",
    "intentos",
    "max_intentos",
    "topic_origen",
    "topic_destino",
    "topic_respuesta",
    "retry_count",
    "dlq",
    "recuperada",
    "backlog_size",
    "recovery_time_ms",
    "escenario",
    "num_consumidores",
    "error",
]


def inicializar_archivo_metricas(ruta_archivo=RUTA_ARCHIVO_METRICAS):
    os.makedirs(os.path.dirname(ruta_archivo), exist_ok=True)

    if not os.path.exists(ruta_archivo):
        escribir_metricas(ruta_archivo, [])
        return

    with open(ruta_archivo, mode="r", newline="", encoding="utf-8") as archivo:
        reader = csv.DictReader(archivo)
        columnas_actuales = reader.fieldnames or []

        if columnas_actuales == COLUMNAS_METRICAS:
            return

        filas = list(reader)

    escribir_metricas(ruta_archivo, filas)


def escribir_metricas(ruta_archivo, filas):
    with open(ruta_archivo, mode="w", newline="", encoding="utf-8") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=COLUMNAS_METRICAS)
        writer.writeheader()

        for fila in filas:
            writer.writerow({columna: fila.get(columna, "") for columna in COLUMNAS_METRICAS})


def registrar_metrica(
    tipo_consulta,
    zona_id=None,
    zona_a=None,
    zona_b=None,
    cache_hit=False,
    latencia_ms=0.0,
    throughput_qps=0.0,
    hubo_eviction=False,
    ttl_segundos=None,
    clave_cache=None,
    origen_respuesta=None,
    consulta_id=None,
    estado=None,
    intentos=0,
    max_intentos=None,
    topic_origen=None,
    topic_destino=None,
    topic_respuesta=None,
    retry_count=0,
    dlq=False,
    recuperada=False,
    backlog_size=None,
    recovery_time_ms=None,
    escenario=None,
    num_consumidores=None,
    error=None,
    ruta_archivo=RUTA_ARCHIVO_METRICAS,
):
    inicializar_archivo_metricas(ruta_archivo)

    fila = {
        "timestamp": time.time(),
        "tipo_consulta": tipo_consulta,
        "zona_id": zona_id,
        "zona_a": zona_a,
        "zona_b": zona_b,
        "cache_hit": cache_hit,
        "latencia_ms": latencia_ms,
        "throughput_qps": throughput_qps,
        "hubo_eviction": hubo_eviction,
        "ttl_segundos": ttl_segundos,
        "clave_cache": clave_cache,
        "origen_respuesta": origen_respuesta,
        "consulta_id": consulta_id,
        "estado": estado,
        "intentos": intentos,
        "max_intentos": max_intentos,
        "topic_origen": topic_origen,
        "topic_destino": topic_destino,
        "topic_respuesta": topic_respuesta,
        "retry_count": retry_count,
        "dlq": dlq,
        "recuperada": recuperada,
        "backlog_size": backlog_size,
        "recovery_time_ms": recovery_time_ms,
        "escenario": escenario,
        "num_consumidores": num_consumidores,
        "error": error,
    }

    with open(ruta_archivo, mode="a", newline="", encoding="utf-8") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=COLUMNAS_METRICAS)
        writer.writerow(fila)


def convertir_float(valor, default=0.0):
    if valor in ("", None, "None"):
        return default
    return float(valor)


def convertir_int(valor, default=0):
    if valor in ("", None, "None"):
        return default
    return int(float(valor))


def convertir_bool(valor):
    return str(valor).lower() == "true"


def leer_metricas(ruta_archivo=RUTA_ARCHIVO_METRICAS):
    metricas = []

    if not os.path.exists(ruta_archivo):
        return metricas

    with open(ruta_archivo, mode="r", newline="", encoding="utf-8") as archivo:
        reader = csv.DictReader(archivo)

        for fila in reader:
            metricas.append({
                "timestamp": convertir_float(fila.get("timestamp")),
                "tipo_consulta": fila.get("tipo_consulta") or None,
                "zona_id": fila.get("zona_id") or None,
                "zona_a": fila.get("zona_a") or None,
                "zona_b": fila.get("zona_b") or None,
                "cache_hit": convertir_bool(fila.get("cache_hit")),
                "latencia_ms": convertir_float(fila.get("latencia_ms")),
                "throughput_qps": convertir_float(fila.get("throughput_qps")),
                "hubo_eviction": convertir_bool(fila.get("hubo_eviction")),
                "ttl_segundos": convertir_int(fila.get("ttl_segundos"), None),
                "clave_cache": fila.get("clave_cache") or None,
                "origen_respuesta": fila.get("origen_respuesta") or None,
                "consulta_id": fila.get("consulta_id") or None,
                "estado": fila.get("estado") or None,
                "intentos": convertir_int(fila.get("intentos")),
                "max_intentos": convertir_int(fila.get("max_intentos"), None),
                "topic_origen": fila.get("topic_origen") or None,
                "topic_destino": fila.get("topic_destino") or None,
                "topic_respuesta": fila.get("topic_respuesta") or None,
                "retry_count": convertir_int(fila.get("retry_count")),
                "dlq": convertir_bool(fila.get("dlq")),
                "recuperada": convertir_bool(fila.get("recuperada")),
                "backlog_size": convertir_int(fila.get("backlog_size"), None),
                "recovery_time_ms": convertir_float(fila.get("recovery_time_ms"), None),
                "escenario": fila.get("escenario") or None,
                "num_consumidores": convertir_int(fila.get("num_consumidores"), None),
                "error": fila.get("error") or None,
            })

    return metricas


def calcular_hit_rate(metricas):
    if not metricas:
        return 0.0

    hits = sum(1 for metrica in metricas if metrica["cache_hit"])
    total = len(metricas)

    return hits / total if total > 0 else 0.0


def calcular_miss_rate(metricas):
    if not metricas:
        return 0.0

    misses = sum(1 for metrica in metricas if not metrica["cache_hit"])
    total = len(metricas)

    return misses / total if total > 0 else 0.0


def calcular_latencia_p50(metricas):
    if not metricas:
        return 0.0

    latencias = sorted(metrica["latencia_ms"] for metrica in metricas)
    return median(latencias)


def calcular_percentil(lista, percentil):
    if not lista:
        return 0.0

    lista_ordenada = sorted(lista)
    indice = (percentil / 100) * (len(lista_ordenada) - 1)

    indice_inferior = int(indice)
    indice_superior = min(indice_inferior + 1, len(lista_ordenada) - 1)

    if indice_inferior == indice_superior:
        return lista_ordenada[indice_inferior]

    peso_superior = indice - indice_inferior
    peso_inferior = 1 - peso_superior

    return (
        lista_ordenada[indice_inferior] * peso_inferior +
        lista_ordenada[indice_superior] * peso_superior
    )


def calcular_latencia_p95(metricas):
    if not metricas:
        return 0.0

    latencias = [metrica["latencia_ms"] for metrica in metricas]
    return calcular_percentil(latencias, 95)


def calcular_throughput_promedio(metricas):
    if not metricas:
        return 0.0

    throughputs = [metrica["throughput_qps"] for metrica in metricas]
    return sum(throughputs) / len(throughputs)


def calcular_eviction_rate(metricas):
    if not metricas:
        return 0.0

    evictions = sum(1 for metrica in metricas if metrica["hubo_eviction"])

    tiempos = [metrica["timestamp"] for metrica in metricas]
    duracion_segundos = max(tiempos) - min(tiempos) if len(tiempos) > 1 else 0

    if duracion_segundos <= 0:
        return 0.0

    duracion_minutos = duracion_segundos / 60
    return evictions / duracion_minutos


def calcular_rate_por_estado(metricas, estado):
    if not metricas:
        return 0.0

    total = len(metricas)
    cantidad = sum(1 for metrica in metricas if metrica["estado"] == estado)
    return cantidad / total if total > 0 else 0.0


def calcular_recovery_rate(metricas):
    consultas_con_retry = {
        metrica["consulta_id"]
        for metrica in metricas
        if metrica["consulta_id"] and metrica["retry_count"] > 0
    }

    if not consultas_con_retry:
        return 0.0

    recuperadas = {
        metrica["consulta_id"]
        for metrica in metricas
        if metrica["consulta_id"] in consultas_con_retry and metrica["estado"] == "completada"
    }

    return len(recuperadas) / len(consultas_con_retry)


def calcular_recovery_time_promedio(metricas):
    tiempos = [
        metrica["recovery_time_ms"]
        for metrica in metricas
        if metrica["recovery_time_ms"] is not None
    ]

    if not tiempos:
        return 0.0

    return sum(tiempos) / len(tiempos)


def calcular_backlog_promedio(metricas):
    valores = [
        metrica["backlog_size"]
        for metrica in metricas
        if metrica["backlog_size"] is not None
    ]

    if not valores:
        return 0.0

    return sum(valores) / len(valores)


def resumen_metricas(ruta_archivo=RUTA_ARCHIVO_METRICAS):
    metricas = leer_metricas(ruta_archivo)

    resumen = {
        "total_eventos": len(metricas),
        "hit_rate": calcular_hit_rate(metricas),
        "miss_rate": calcular_miss_rate(metricas),
        "latencia_p50_ms": calcular_latencia_p50(metricas),
        "latencia_p95_ms": calcular_latencia_p95(metricas),
        "throughput_promedio_qps": calcular_throughput_promedio(metricas),
        "eviction_rate_por_minuto": calcular_eviction_rate(metricas),
        "retry_rate": calcular_rate_por_estado(metricas, "retry"),
        "dlq_rate": calcular_rate_por_estado(metricas, "dlq"),
        "recovery_rate": calcular_recovery_rate(metricas),
        "recovery_time_promedio_ms": calcular_recovery_time_promedio(metricas),
        "backlog_promedio": calcular_backlog_promedio(metricas),
    }

    return resumen


def main():
    inicializar_archivo_metricas()
    print("Archivo de metricas inicializado.")
    print("Resumen de metricas:")
    print(resumen_metricas())


if __name__ == "__main__":
    main()
