import os
import math
import random
import time
import pandas as pd

from fastapi import FastAPI

app = FastAPI()

# --------------------------------------
# Variables globales cargadas al iniciar
# --------------------------------------
datos_por_zona = None
areas_por_zona = None

# -----------------------------
# Ruta al dataset
# -----------------------------
RUTA_CSV = os.getenv("RUTA_CSV", "967_buildings.csv")

SIMULAR_FALLOS = os.getenv("SIMULAR_FALLOS", "false").lower() == "true"
PROBABILIDAD_FALLO = float(os.getenv("PROBABILIDAD_FALLO", "0.0"))
LATENCIA_ARTIFICIAL_MS = int(os.getenv("LATENCIA_ARTIFICIAL_MS", "0"))

# -----------------------------
# Zonas predefinidas del PDF
# -----------------------------
ZONAS = {
    "Z1": {
        "nombre": "Providencia",
        "lat_min": -33.445,
        "lat_max": -33.420,
        "lon_min": -70.640,
        "lon_max": -70.600
    },
    "Z2": {
        "nombre": "Las Condes",
        "lat_min": -33.420,
        "lat_max": -33.390,
        "lon_min": -70.600,
        "lon_max": -70.550
    },
    "Z3": {
        "nombre": "Maipú",
        "lat_min": -33.530,
        "lat_max": -33.490,
        "lon_min": -70.790,
        "lon_max": -70.740
    },
    "Z4": {
        "nombre": "Santiago Centro",
        "lat_min": -33.460,
        "lat_max": -33.430,
        "lon_min": -70.670,
        "lon_max": -70.630
    },
    "Z5": {
        "nombre": "Pudahuel",
        "lat_min": -33.470,
        "lat_max": -33.430,
        "lon_min": -70.810,
        "lon_max": -70.760
    }
}

# -----------------------------
# Cargar dataset
# -----------------------------
def cargar_dataset(ruta_csv):
    columnas_necesarias = ["latitude", "longitude", "area_in_meters", "confidence"]
    chunks_filtrados = []

    try:
        lector_csv = pd.read_csv(
            ruta_csv,
            usecols=columnas_necesarias,
            chunksize=200_000
        )

        for chunk in lector_csv:
            for columna in columnas_necesarias:
                chunk[columna] = pd.to_numeric(chunk[columna], errors="coerce")

            chunk = chunk.dropna()

            mascara_zonas = False
            for zona in ZONAS.values():
                mascara_zonas = mascara_zonas | (
                    (chunk["latitude"] >= zona["lat_min"]) &
                    (chunk["latitude"] <= zona["lat_max"]) &
                    (chunk["longitude"] >= zona["lon_min"]) &
                    (chunk["longitude"] <= zona["lon_max"])
                )

            chunk_filtrado = chunk[mascara_zonas].copy()
            if not chunk_filtrado.empty:
                chunks_filtrados.append(chunk_filtrado)

    except ValueError as exc:
        raise ValueError(f"Falta una columna requerida en el CSV: {exc}")

    if not chunks_filtrados:
        return pd.DataFrame(columns=columnas_necesarias)

    return pd.concat(chunks_filtrados, ignore_index=True)

# --------------------------------------
# Calcular área aproximada de bbox en km²
# ---------------------------------------
def calcular_area_bbox_km2(lat_min, lat_max, lon_min, lon_max):
    lat_centro = (lat_min + lat_max) / 2.0

    km_por_grado_lat = 111.32
    km_por_grado_lon = 111.32 * math.cos(math.radians(lat_centro))

    alto_km = abs(lat_max - lat_min) * km_por_grado_lat
    ancho_km = abs(lon_max - lon_min) * km_por_grado_lon

    return alto_km * ancho_km

# -----------------------------
# Separar datos por zona
# -----------------------------
def separar_datos_por_zona(df):
    datos = {}
    areas = {}

    for zona_id, zona in ZONAS.items():
        df_zona = df[
            (df["latitude"] >= zona["lat_min"]) &
            (df["latitude"] <= zona["lat_max"]) &
            (df["longitude"] >= zona["lon_min"]) &
            (df["longitude"] <= zona["lon_max"])
        ].copy()

        datos[zona_id] = df_zona

        areas[zona_id] = calcular_area_bbox_km2(
            zona["lat_min"],
            zona["lat_max"],
            zona["lon_min"],
            zona["lon_max"]
        )

    return datos, areas

# -----------------------------
# Q1
# -----------------------------
def consulta_q1(datos, zona_id, confianza_min=0.0):
    if zona_id not in datos:
        return {"error": f"Zona inválida: {zona_id}"}

    df_zona = datos[zona_id]
    df_filtrado = df_zona[df_zona["confidence"] >= confianza_min]

    return {
        "tipo_consulta": "Q1",
        "zona_id": zona_id,
        "confianza_min": confianza_min,
        "cantidad_edificios": int(len(df_filtrado))
    }

# -----------------------------
# Q2
# -----------------------------
def consulta_q2(datos, zona_id, confianza_min=0.0):
    if zona_id not in datos:
        return {"error": f"Zona inválida: {zona_id}"}

    df_zona = datos[zona_id]
    df_filtrado = df_zona[df_zona["confidence"] >= confianza_min]

    if df_filtrado.empty:
        return {
            "tipo_consulta": "Q2",
            "zona_id": zona_id,
            "confianza_min": confianza_min,
            "area_promedio": 0.0,
            "area_total": 0.0,
            "cantidad_edificios": 0
        }

    return {
        "tipo_consulta": "Q2",
        "zona_id": zona_id,
        "confianza_min": confianza_min,
        "area_promedio": float(df_filtrado["area_in_meters"].mean()),
        "area_total": float(df_filtrado["area_in_meters"].sum()),
        "cantidad_edificios": int(len(df_filtrado))
    }

# -----------------------------
# Q3
# -----------------------------
def consulta_q3(datos, areas, zona_id, confianza_min=0.0):
    if zona_id not in datos:
        return {"error": f"Zona inválida: {zona_id}"}

    cantidad = consulta_q1(datos, zona_id, confianza_min)["cantidad_edificios"]
    area_km2 = areas[zona_id]
    densidad = cantidad / area_km2 if area_km2 > 0 else 0.0

    return {
        "tipo_consulta": "Q3",
        "zona_id": zona_id,
        "confianza_min": confianza_min,
        "densidad_edificios_km2": float(densidad)
    }

# -----------------------------
# Q4
# -----------------------------
def consulta_q4(datos, areas, zona_a, zona_b, confianza_min=0.0):
    if zona_a not in datos or zona_b not in datos:
        return {"error": "Una o ambas zonas son inválidas"}

    densidad_a = consulta_q3(datos, areas, zona_a, confianza_min)["densidad_edificios_km2"]
    densidad_b = consulta_q3(datos, areas, zona_b, confianza_min)["densidad_edificios_km2"]

    if densidad_a > densidad_b:
        ganador = zona_a
    elif densidad_b > densidad_a:
        ganador = zona_b
    else:
        ganador = "empate"

    return {
        "tipo_consulta": "Q4",
        "zona_a": zona_a,
        "zona_b": zona_b,
        "confianza_min": confianza_min,
        "densidad_zona_a": float(densidad_a),
        "densidad_zona_b": float(densidad_b),
        "zona_ganadora": ganador
    }

# -----------------------------
# Q5
# -----------------------------
def consulta_q5(datos, zona_id, bins=5):
    if zona_id not in datos:
        return {"error": f"Zona inválida: {zona_id}"}

    if bins <= 0:
        return {"error": "bins debe ser mayor que 0"}

    df_zona = datos[zona_id]

    if df_zona.empty:
        return {
            "tipo_consulta": "Q5",
            "zona_id": zona_id,
            "bins": bins,
            "distribucion": []
        }

    conteos, bordes = pd.cut(
        df_zona["confidence"],
        bins=bins,
        include_lowest=True,
        retbins=True
    )

    frecuencias = conteos.value_counts(sort=False)

    distribucion = []
    for i, cantidad in enumerate(frecuencias):
        distribucion.append({
            "bucket": i,
            "min": float(bordes[i]),
            "max": float(bordes[i + 1]),
            "cantidad": int(cantidad)
        })

    return {
        "tipo_consulta": "Q5",
        "zona_id": zona_id,
        "bins": bins,
        "distribucion": distribucion
    }


def aplicar_condiciones_experimentales():
    if LATENCIA_ARTIFICIAL_MS > 0:
        time.sleep(LATENCIA_ARTIFICIAL_MS / 1000)

    if SIMULAR_FALLOS and random.random() < PROBABILIDAD_FALLO:
        raise RuntimeError("Fallo temporal simulado en el generador de respuestas")

# -----------------------------
# Resolver consulta
# -----------------------------
def responder_consulta(consulta, datos, areas):
    aplicar_condiciones_experimentales()

    if not isinstance(consulta, dict):
        return {"error": "La consulta debe ser un diccionario"}

    tipo_consulta = consulta.get("tipo_consulta")
    parametros = consulta.get("parametros", {})

    if tipo_consulta is None:
        return {"error": "Falta 'tipo_consulta' en la consulta"}

    if not isinstance(parametros, dict):
        return {"error": "'parametros' debe ser un diccionario"}

    try:
        if tipo_consulta == "Q1":
            zona_id = consulta.get("zona_id")
            confianza_min = float(parametros.get("confianza_min", 0.0))
            return consulta_q1(datos, zona_id, confianza_min)

        elif tipo_consulta == "Q2":
            zona_id = consulta.get("zona_id")
            confianza_min = float(parametros.get("confianza_min", 0.0))
            return consulta_q2(datos, zona_id, confianza_min)

        elif tipo_consulta == "Q3":
            zona_id = consulta.get("zona_id")
            confianza_min = float(parametros.get("confianza_min", 0.0))
            return consulta_q3(datos, areas, zona_id, confianza_min)

        elif tipo_consulta == "Q4":
            zona_a = parametros.get("zona_a")
            zona_b = parametros.get("zona_b")
            confianza_min = float(parametros.get("confianza_min", 0.0))
            return consulta_q4(datos, areas, zona_a, zona_b, confianza_min)

        elif tipo_consulta == "Q5":
            zona_id = consulta.get("zona_id")
            bins = int(parametros.get("bins", 5))
            return consulta_q5(datos, zona_id, bins)

        else:
            return {"error": f"Tipo de consulta no soportado: {tipo_consulta}"}

    except Exception as e:
        return {"error": f"Error al procesar la consulta: {str(e)}"}

# -----------------------------
# Startup
# -----------------------------
@app.on_event("startup")
def iniciar_servicio():
    global datos_por_zona, areas_por_zona

    print("Cargando dataset en memoria...")
    df = cargar_dataset(RUTA_CSV)
    datos_por_zona, areas_por_zona = separar_datos_por_zona(df)
    print("Dataset cargado correctamente.")

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    return {
        "servicio": "generador_respuestas",
        "dataset_cargado": datos_por_zona is not None,
        "simular_fallos": SIMULAR_FALLOS,
        "probabilidad_fallo": PROBABILIDAD_FALLO,
        "latencia_artificial_ms": LATENCIA_ARTIFICIAL_MS
    }

@app.post("/resolver")
def resolver(consulta: dict):
    return responder_consulta(consulta, datos_por_zona, areas_por_zona)
