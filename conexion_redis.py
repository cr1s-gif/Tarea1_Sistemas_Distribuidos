   import redis


def crear_cliente_redis(host="localhost", puerto=6379, db=0):
    cliente = redis.Redis(
        host=host,
        port=puerto,
        db=db,
        decode_responses=True
    )
    return cliente


def probar_conexion(cliente):
    try:
        cliente.ping()
        return True
    except redis.ConnectionError:
        return False


def limpiar_cache(cliente):
    cliente.flushdb()


def obtener_ttl(cliente, clave):
    return cliente.ttl(clave)


def obtener_valor(cliente, clave):
    return cliente.get(clave)


def guardar_valor(cliente, clave, valor_json, ttl_segundos):
    cliente.setex(clave, ttl_segundos, valor_json)
