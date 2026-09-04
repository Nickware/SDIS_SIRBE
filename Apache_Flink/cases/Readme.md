# Prueba de funcionamiento de Flink

Este caso comprueba que PyFlink puede crear un flujo, procesar eventos y
emitir una confirmación final. El flujo tiene tres etapas lógicas:

1. Saluda desde Flink.
2. Calcula una pequeña caída libre con $g = 9.8\ \text{m/s}^2$.
3. Emite la confirmación solo cuando las dos etapas anteriores terminan con éxito.

En Flink, estas etapas se representan como transformaciones de un `DataStream`,
no como tareas independientes de un DAG de Airflow.

## Ejecutar la prueba

Desde el directorio que contiene el archivo:

```bash
cd Apache_Flink/cases

$FLINK_HOME/bin/flink run \
  -Dpython.client.executable="$FLINK_PYTHON_ENV/bin/python" \
  -py physics_prueba.py
```

Si se ejecuta Flink desde otra ubicación, se puede usar la ruta absoluta del
archivo `physics_prueba.py`.

## Verificar el resultado

El job debe terminar correctamente y mostrar en la salida una línea parecida a:

```text
('confirmacion_final', True, 'Hola desde Apache Flink | Caida libre desde 20.0 m: t=2.02 s, v=19.80 m/s')
```

También se puede comprobar el job en la interfaz web de Flink:

```text
http://localhost:8081
```

La salida no contiene la confirmación si alguna de las dos ramas produce un
resultado fallido. Esto permite comprobar la condición equivalente a la
dependencia `[tarea_inicio, tarea_calculo] >> tarea_fin` del ejemplo de Airflow.