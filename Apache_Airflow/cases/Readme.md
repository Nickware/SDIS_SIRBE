# Prueba de funcionamiento de Airflow

Para comprobar que todo el engranaje (el backend, el programador y los hilos de ejecución) funciona correctamente, se debe crear un **DAG** (Direct Acyclic Graph) clásico de prueba. En Airflow 3.0, la estructura base se mantiene, pero se beneficia de la limpieza moderna de Python.

Seguir estos pasos para poner a correr tu primer flujo de trabajo:

### 1. Crear la carpeta de proyectos

Airflow busca los flujos de trabajo en una carpeta llamada `dags` dentro de tu directorio de trabajo. Si estás usando la ruta por defecto, abre otra terminal dentro de tu Distrobox y ejecuta:

```bash
mkdir -p ~/airflow/dags

```

### 2. Crear el archivo del DAG

Crea un archivo de Python dentro de esa carpeta. Puedes usar `nano` o tu editor preferido:

```bash
nano ~/airflow/dags/dag_prueba.py

```

Pegar el siguiente código dentro del archivo. Es un flujo muy sencillo con tres tareas: la primera saluda, la segunda simula un cálculo físico rápido (una pequeña caída libre usando la gravedad $g = 9.8 \text{ m/s}^2$) y la tercera se ejecuta solo si las dos anteriores terminan con éxito.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 1. Definición de argumentos por defecto
default_args = {
    'owner': 'n_torres',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 2. Inicializar el DAG
with DAG(
    dag_id='mi_primer_dag_prueba',
    default_args=default_args,
    description='Un DAG sencillo para verificar el entorno en Distrobox',
    schedule=None,  # 'None' significa que solo se ejecuta manualmente
    catchup=False,
    tags=['prueba', 'distrobox'],
) as dag:

    # Tarea 1: Un comando Bash simple
    tarea_inicio = BashOperator(
        task_id='saludo_inicial',
        bash_command='echo "¡Hola desde el contenedor de Ubuntu en Distrobox!"',
    )

    # Tarea 2: Una función de Python
    def simular_calculo():
        g = 9.8
        t = 5.0
        distancia = 0.5 * g * (t ** 2)
        print(f"Simulación completada. Distancia calculada en {t}s: {distancia} metros.")
        return distancia

    tarea_calculo = PythonOperator(
        task_id='calculo_fisico',
        python_callable=simular_calculo,
    )

    # Tarea 3: Confirmación final
    tarea_fin = BashOperator(
        task_id='confirmacion_final',
        bash_command='echo "El flujo terminó exitosamente a las $(date)"',
    )

    # 3. Definir el orden de ejecución (Flujo de dependencias)
    [tarea_inicio, tarea_calculo] >> tarea_fin

```

Guardar el archivo (`Ctrl + O`, `Enter` y luego `Ctrl + X` en nano).

---

### 3. Probarlo en la Interfaz Web

1. Ir al navegador e ingresar a `http://localhost:9090`.
2. En la pestaña principal (**DAGs**), dar un par de segundos (o refrescar la página) y deberá aparecer **`mi_primer_dag_prueba`**.
3. Notará que el DAG está en modo "pausado" (un switch azul/gris a la izquierda). **Actívalo** cambiando el switch a azul.
4. A la derecha, haz clic en el botón de **Play (Trigger DAG)** para forzar el inicio manual.

### 4. ¿Cómo verificar que funcionó bien?

* Hacer clic sobre el nombre del DAG para entrar a ver los detalles.
* Ir a la vista de **Graph** o **Grid**. Deberá ver los tres cuadrados de las tareas ponerse en color verde claro (significa *Success* / Éxito).
* Si se hace clic sobre la tarea `calculo_fisico` y luego selecciona **Logs**, podrá ver impreso el resultado de la distancia calculada por la función de Python en las líneas de la terminal interna de Airflow.

Con esto habrá verificado que tu instalación no solo levanta la interfaz, sino que el *Scheduler* y los *Workers* están listos para ejecutar tareas complejas de cómputo y automatización. ¡Debera cambiar a verde!

## Deshabilitar los ejemplos por defecto (recomendado)

Si ve los DAGs de ejemplo que Airflow trae por defecto (como `example_bash_operator`), pero el suyo no aparece, no se preocupe: es el comportamiento típico de una instalación nueva.

Esto se debe a dos razones principales:

### 1. El Scheduler (Programador) no está corriendo

Para que la interfaz web se entere de que creaste un archivo nuevo, necesita que el proceso **`airflow scheduler`** esté ejecutándose en segundo plano. El servidor web por sí solo no busca archivos nuevos en el disco; solo lee lo que el Scheduler registra en la base de datos.

* **Si está usando `airflow standalone`:** Este comando arranca tanto el webserver como el scheduler en la misma ventana. Asegúrarse de que no se haya congelado el proceso.
* **Si levanta solo el webserver:** Abrir una pestaña nueva **dentro del contenedor Distrobox**, activar el entorno virtual (`source ~/airflow_venv/bin/activate`) y arrancar el programador manualmente con:
```bash
airflow scheduler

```

### 2. Airflow está buscando en la ruta equivocada

Al instalar Airflow desde cero, por defecto busca los DAGs en `~/airflow/dags`. Sin embargo, vale la pena verificar si realmente se guardo el archivo ahí o si Airflow está mirando otra carpeta.

Puede comprobar qué ruta exacta está leyendo la configuración, corriendo esto en la terminal del contenedor:

```bash
airflow config get-value core dags_folder

```

El resultado debe coincidir exactamente con la ruta donde se creo el archivo `dag_prueba.py`. Si devuelve una ruta distinta (por ejemplo, `/root/airflow/dags` o algo similar), mover el archivo a esa ubicación exacta.

---

### 3. Error de sintaxis oculto (Error de parseo)

Si el Scheduler ya está corriendo y la ruta es la correcta, es posible que haya un pequeño error de código en el archivo de Python. Cuando esto pasa, Airflow oculta el DAG para que no rompa la interfaz, pero avisa en la parte superior de la pantalla.

* Mira la barra superior de la interfaz web en su navegador. Si hay un error, verás un **botón rojo parpadeante o un recuadro de alerta** que dice **"DAG Import Errors"**. Si hace clic ahí, dirá exactamente en qué línea falló el script de Python.

### Una última sugerencia de limpieza:

Para deshacerte de ese montón de ejemplos que saturan la pantalla y encontrar el suyo más rápido, puede desactivarlos modificando el archivo de configuración `~/airflow/airflow.cfg`:

1. Abrir el archivo: `nano ~/airflow/airflow.cfg`
2. Buscar la línea: `load_examples = True`
3. Cámbialar a: `load_examples = False`
4. Guarda el archivo, reiniciar su comando `airflow standalone` (o el webserver/scheduler) y la lista quedará limpia, esperando únicamente sus propios flujos de trabajo.