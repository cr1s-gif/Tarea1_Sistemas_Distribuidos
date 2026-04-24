import os
import random
import time
import requests

URL_CACHE = os.getenv("URL_CACHE", "http://localhost:8000/consulta")
INTERVALO_SEGUNDOS = int(os.getenv("INTERVALO_SEGUNDOS", "10"))

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
    distribucion = os.getenv("DISTRIBUCION", "uniforme").lower()

    if distribucion not in ["uniforme", "zipf"]:
        print("Distribución inválida, se usará uniforme por defecto")
        return "uniforme"

    return distribucion

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

def main():
    distribucion = obtener_distribucion()
    print(f"Usando distribución: {distribucion.upper()}")
    print(f"Enviando consultas a: {URL_CACHE}")

    while True:
        consulta = generar_consulta(distribucion)

        print("Consulta generada:")
        print(consulta)

        try:
            resultado = enviar_consulta_al_cache(consulta)
            print("Respuesta recibida:")
            print(resultado)
        except Exception as e:
            print(f"Error al enviar consulta al caché: {e}")

        print("-" * 60)
        time.sleep(INTERVALO_SEGUNDOS)

if __name__ == "__main__":
    main()
