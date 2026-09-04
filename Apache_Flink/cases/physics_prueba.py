import math

from pyflink.datastream import StreamExecutionEnvironment


def saludo(_evento):
    return ("saludo", True, "Hola desde Apache Flink")


def calcular_caida_libre(_evento):
    gravedad = 9.8
    altura = 20.0
    tiempo = math.sqrt(2 * altura / gravedad)
    velocidad = gravedad * tiempo
    mensaje = (
        f"Caida libre desde {altura} m: "
        f"t={tiempo:.2f} s, v={velocidad:.2f} m/s"
    )
    return ("calculo_fisico", tiempo > 0 and velocidad > 0, mensaje)


def reunir_resultados(resultado_a, resultado_b):
    exitoso = resultado_a[1] and resultado_b[1]
    mensajes = f"{resultado_a[2]} | {resultado_b[2]}"
    return ("confirmacion_final", exitoso, mensajes)


def main():
    entorno = StreamExecutionEnvironment.get_execution_environment()

    eventos = entorno.from_collection([0])
    saludo_resultado = eventos.map(saludo)
    calculo_resultado = eventos.map(calcular_caida_libre)

    confirmacion = (
        saludo_resultado.union(calculo_resultado)
        .key_by(lambda resultado: "prueba_fisica")
        .reduce(reunir_resultados)
        .filter(lambda resultado: resultado[0] == "confirmacion_final" and resultado[1])
    )

    confirmacion.print()
    entorno.execute("Prueba de funcionamiento de fisica en Flink")


if __name__ == "__main__":
    main()