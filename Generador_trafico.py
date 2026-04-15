import random
import time

# Zonas disponibles
ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

# -----------------------------
# Q1 — Conteo de edificios
# -----------------------------
def consulta_q1():
    return {
        "tipo_consulta": "Q1",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "confianza_min": round(random.uniform(0.0, 1.0), 2)
        }
    }

# -----------------------------
# Q2 — Área promedio y total
# -----------------------------
def consulta_q2():
    return {
        "tipo_consulta": "Q2",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "confianza_min": round(random.uniform(0.0, 1.0), 2)
        }
    }

# -----------------------------
# Q3 — Densidad
# -----------------------------
def consulta_q3():
    return {
        "tipo_consulta": "Q3",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "confianza_min": round(random.uniform(0.0, 1.0), 2)
        }
    }

# -----------------------------
# Q4 — Comparación entre zonas
# -----------------------------
def consulta_q4():
    zona_a, zona_b = random.sample(ZONAS, 2)
    return {
        "tipo_consulta": "Q4",
        "parametros": {
            "zona_a": zona_a,
            "zona_b": zona_b,
            "confianza_min": round(random.uniform(0.0, 1.0), 2)
        }
    }

# -----------------------------
# Q5 — Distribución de confianza
# -----------------------------
def consulta_q5():
    return {
        "tipo_consulta": "Q5",
        "zona_id": random.choice(ZONAS),
        "parametros": {
            "bins": random.choice([5, 10])
        }
    }

# Lista de consultas
CONSULTAS = [consulta_q1, consulta_q2, consulta_q3, consulta_q4, consulta_q5]

# Pesos para distribución Zipf
PESOS_ZIPF = [1 / (i + 1) for i in range(len(CONSULTAS))]


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("Seleccione tipo de distribución:")
    print("1 -> Uniforme")
    print("2 -> Zipf")

    opcion = input("Ingrese opción: ")

    if opcion == "1":
        distribucion = "uniforme"
    elif opcion == "2":
        distribucion = "zipf"
    else:
        print("Opción inválida, se usará uniforme por defecto")
        distribucion = "uniforme"

    print(f"\nUsando distribución: {distribucion.upper()}\n")

    while True:
        # Selección según distribución
        if distribucion == "uniforme":
            funcion_consulta = random.choice(CONSULTAS)

        elif distribucion == "zipf":
            funcion_consulta = random.choices(CONSULTAS, weights=PESOS_ZIPF, k=1)[0]

        # Generar consulta
        consulta = funcion_consulta()

        print("Consulta generada:")
        print(consulta)
        print("-" * 40)

        time.sleep(10)


if __name__ == "__main__":
    main()
