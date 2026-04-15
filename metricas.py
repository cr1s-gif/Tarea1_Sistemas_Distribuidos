import csv
import os
import time
from statistics import median


RUTA_ARCHIVO_METRICAS = "metricas.csv"


def inicializar_archivo_metricas(ruta_archivo=RUTA_ARCHIVO_METRICAS):
    if not os.path.exists(ruta_archivo):
        with open(ruta_archivo, mode="w", newline="", encoding="utf-8") as archivo:
            writer = csv.writer(archivo)
            writer.writerow([
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
                "origen_respuesta"
            ])


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
    ruta_archivo=RUTA_ARCHIVO_METRICAS
):
    timestamp = time.time()

    with open(ruta_archivo, mode="a", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow([
            timestamp,
            tipo_consulta,
            zona_id,
            zona_a,
            zona_b,
            cache_hit,
            latencia_ms,
            throughput_qps,
            hubo_eviction,
            ttl_segundos,
            clave_cache,
            origen_respuesta
        ])


def leer_metricas(ruta_archivo=RUTA_ARCHIVO_METRICAS):
    metricas = []

    if not os.path.exists(ruta_archivo):
        return metricas

    with open(ruta_archivo, mode="r", newline="", encoding="utf-8") as archivo:
        reader = csv.DictReader(archivo)

        for fila in reader:
            metricas.append({
                "timestamp": float(fila["timestamp"]),
                "tipo_consulta": fila["tipo_consulta"],
                "zona_id": fila["zona_id"] if fila["zona_id"] else None,
                "zona_a": fila["zona_a"] if fila["zona_a"] else None,
                "zona_b": fila["zona_b"] if fila["zona_b"] else None,
                "cache_hit": fila["cache_hit"] == "True",
                "latencia_ms": float(fila["latencia_ms"]),
                "throughput_qps": float(fila["throughput_qps"]),
                "hubo_eviction": fila["hubo_eviction"] == "True",
                "ttl_segundos": int(fila["ttl_segundos"]) if fila["ttl_segundos"] not in ("", "None") else None,
                "clave_cache": fila["clave_cache"] if fila["clave_cache"] else None,
                "origen_respuesta": fila["origen_respuesta"] if fila["origen_respuesta"] else None
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


def resumen_metricas(ruta_archivo=RUTA_ARCHIVO_METRICAS):
    metricas = leer_metricas(ruta_archivo)

    resumen = {
        "total_consultas": len(metricas),
        "hit_rate": calcular_hit_rate(metricas),
        "miss_rate": calcular_miss_rate(metricas),
        "latencia_p50_ms": calcular_latencia_p50(metricas),
        "latencia_p95_ms": calcular_latencia_p95(metricas),
        "throughput_promedio_qps": calcular_throughput_promedio(metricas),
        "eviction_rate_por_minuto": calcular_eviction_rate(metricas)
    }

    return resumen


def main():
    inicializar_archivo_metricas()

    print("Archivo de métricas inicializado.")

    registrar_metrica(
        tipo_consulta="Q1",
        zona_id="Z1",
        cache_hit=False,
        latencia_ms=12.4,
        throughput_qps=0.1,
        hubo_eviction=False,
        ttl_segundos=120,
        clave_cache="count:Z1:conf=0.5",
        origen_respuesta="generador_respuestas"
    )

    registrar_metrica(
        tipo_consulta="Q1",
        zona_id="Z1",
        cache_hit=True,
        latencia_ms=1.8,
        throughput_qps=0.1,
        hubo_eviction=False,
        ttl_segundos=120,
        clave_cache="count:Z1:conf=0.5",
        origen_respuesta="cache"
    )

    print("Métricas de prueba registradas.\n")

    resumen = resumen_metricas()
    print("Resumen de métricas:")
    print(resumen)


if __name__ == "__main__":
    main()