import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

import requests


URL_CACHE = os.getenv("URL_CACHE", "http://localhost:8000/consulta")
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", "10"))
DISTRIBUCION = os.getenv("DISTRIBUCION", "uniforme").lower()

MODO_TRAFICO = os.getenv("MODO_TRAFICO", "normal").lower()
CONSULTAS_POR_SEGUNDO = float(os.getenv("CONSULTAS_POR_SEGUNDO", "0"))
SPIKE_CONSULTAS_POR_SEGUNDO = float(os.getenv("SPIKE_CONSULTAS_POR_SEGUNDO", "10"))
DURACION_SPIKE_SEGUNDOS = int(os.getenv("DURACION_SPIKE_SEGUNDOS", "30"))
DURACION_TOTAL_SEGUNDOS = int(os.getenv("DURACION_TOTAL_SEGUNDOS", "0"))
CONCURRENCIA = int(os.getenv("CONCURRENCIA", "10"))

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]


def consulta_q1():
    return {
        "tipo_consulta": "Q1",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "confianza_min": random.choice([0.0, 0.3, 0.5, 0.7])
        }
    }


def consulta_q2():
    return {
        "tipo_consulta": "Q2",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "confianza_min": random.choice([0.0, 0.3, 0.5, 0.7])
        }
    }


def consulta_q3():
    return {
        "tipo_consulta": "Q3",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "confianza_min": random.choice([0.0, 0.3, 0.5, 0.7])
        }
    }


def consulta_q4():
    zona_a, zona_b = random.sample(ZONAS, 2)
    return {
        "tipo_consulta": "Q4",
        "parametros": {
            "zona_a": zona_a,
            "zona_b": zona_b,
            "confianza_min": random.choice([0.0, 0.3, 0.5, 0.7])
        }
    }


def consulta_q5():
    return {
        "tipo_consulta": "Q5",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "bins": random.choice([5, 10])
        }
    }


CONSULTAS = [consulta_q1, consulta_q2, consulta_q3, consulta_q4, consulta_q5]
PESOS_ZIPF = [1 / (i + 1) for i in range(len(CONSULTAS))]


def obtener_distribucion():
    if DISTRIBUCION not in ["uniforme", "zipf"]:
        print("Distribucion invalida, se usara uniforme por defecto")
        return "uniforme"

    return DISTRIBUCION


def obtener_modo_trafico():
    if MODO_TRAFICO not in ["normal", "alta_carga", "spike"]:
        print("Modo de trafico invalido, se usara normal por defecto")
        return "normal"

    return MODO_TRAFICO


def obtener_qps_base(modo):
    if CONSULTAS_POR_SEGUNDO > 0:
        return CONSULTAS_POR_SEGUNDO

    if modo == "alta_carga":
        return 10

    return 1 / INTERVALO_SEGUNDOS if INTERVALO_SEGUNDOS > 0 else 1


def obtener_qps_actual(modo, segundos_transcurridos, qps_base):
    if modo == "spike" and segundos_transcurridos < DURACION_SPIKE_SEGUNDOS:
        return SPIKE_CONSULTAS_POR_SEGUNDO

    return qps_base


def generar_consulta(distribucion):
    if distribucion == "uniforme":
        funcion_consulta = random.choice(CONSULTAS)
    else:
        funcion_consulta = random.choices(CONSULTAS, weights=PESOS_ZIPF, k=1)[0]

    return funcion_consulta()


def enviar_consulta_al_cache(consulta):
    respuesta = requests.post(URL_CACHE, json=consulta, timeout=30)
    respuesta.raise_for_status()
    return respuesta.json()


def enviar_y_mostrar_resultado(consulta):
    try:
        resultado = enviar_consulta_al_cache(consulta)
        print("Consulta enviada:")
        print(consulta)
        print("Respuesta recibida:")
        print(resultado)
    except Exception as e:
        print(f"Error al enviar consulta al cache: {e}")

    print("-" * 60)


def main():
    distribucion = obtener_distribucion()
    modo = obtener_modo_trafico()
    qps_base = obtener_qps_base(modo)
    inicio = time.time()

    print(f"Usando distribucion: {distribucion.upper()}")
    print(f"Modo de trafico: {modo.upper()}")
    print(f"QPS base: {qps_base}")
    if modo == "spike":
        print(f"QPS spike: {SPIKE_CONSULTAS_POR_SEGUNDO}")
        print(f"Duracion spike: {DURACION_SPIKE_SEGUNDOS}s")
    if DURACION_TOTAL_SEGUNDOS > 0:
        print(f"Duracion total: {DURACION_TOTAL_SEGUNDOS}s")
    print(f"Concurrencia maxima: {CONCURRENCIA}")
    print(f"Enviando consultas a: {URL_CACHE}")

    with ThreadPoolExecutor(max_workers=CONCURRENCIA) as executor:
        while True:
            segundos_transcurridos = time.time() - inicio

            if DURACION_TOTAL_SEGUNDOS > 0 and segundos_transcurridos >= DURACION_TOTAL_SEGUNDOS:
                print("Duracion total alcanzada. Finalizando generador de trafico.")
                break

            qps_actual = obtener_qps_actual(modo, segundos_transcurridos, qps_base)
            intervalo = 1 / qps_actual if qps_actual > 0 else INTERVALO_SEGUNDOS
            consulta = generar_consulta(distribucion)

            executor.submit(enviar_y_mostrar_resultado, consulta)
            time.sleep(intervalo)


if __name__ == "__main__":
    main()
