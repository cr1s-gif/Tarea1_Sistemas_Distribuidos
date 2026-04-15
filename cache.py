import json
from conexion_redis import crear_cliente_redis, probar_conexion
from generador_respuestas import cargar_dataset, separar_datos_por_zona, responder_consulta


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


def buscar_en_cache(cliente_redis, clave):
    valor = cliente_redis.get(clave)

    if valor is None:
        return None

    return json.loads(valor)


def guardar_en_cache(cliente_redis, clave, respuesta, ttl_segundos=60):
    respuesta_json = json.dumps(respuesta)
    cliente_redis.setex(clave, ttl_segundos, respuesta_json)


def procesar_consulta_cache(consulta, cliente_redis, datos_por_zona, areas_por_zona, ttl_segundos=60):
    clave_cache = construir_clave_cache(consulta)

    if clave_cache is None:
        return {"error": "No se pudo construir la clave de caché"}

    respuesta_cache = buscar_en_cache(cliente_redis, clave_cache)

    if respuesta_cache is not None:
        return {
            "cache_hit": True,
            "origen": "cache",
            "clave_cache": clave_cache,
            "respuesta": respuesta_cache
        }

    respuesta_generada = responder_consulta(consulta, datos_por_zona, areas_por_zona)

    guardar_en_cache(cliente_redis, clave_cache, respuesta_generada, ttl_segundos)

    return {
        "cache_hit": False,
        "origen": "generador_respuestas",
        "clave_cache": clave_cache,
        "respuesta": respuesta_generada
    }


def main():
    ruta_csv = "datos.csv"

    print("Cargando dataset...")
    df = cargar_dataset(ruta_csv)
    datos_por_zona, areas_por_zona = separar_datos_por_zona(df)

    print("Conectando a Redis...")
    cliente_redis = crear_cliente_redis(host="localhost", puerto=6379, db=0)

    if not probar_conexion(cliente_redis):
        print("No se pudo conectar a Redis.")
        return

    print("Conexión con Redis exitosa.\n")

    consulta_prueba = {
        "tipo_consulta": "Q1",
        "zona_id": "Z1",
        "parametros": {
            "confianza_min": 0.5
        }
    }

    print("Primera consulta:")
    resultado_1 = procesar_consulta_cache(
        consulta_prueba,
        cliente_redis,
        datos_por_zona,
        areas_por_zona,
        ttl_segundos=120
    )
    print(resultado_1)
    print()

    print("Segunda consulta:")
    resultado_2 = procesar_consulta_cache(
        consulta_prueba,
        cliente_redis,
        datos_por_zona,
        areas_por_zona,
        ttl_segundos=120
    )
    print(resultado_2)


if __name__ == "__main__":
    main()