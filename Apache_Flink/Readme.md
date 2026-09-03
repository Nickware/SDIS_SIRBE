# Apache Flink

**Apache Flink** es un framework de código abierto para el procesamiento de flujos de datos (*stream processing*) a gran escala. A diferencia de otras tecnologías que tratan el flujo continuo de datos como una serie de pequeños lotes, Flink fue diseñado desde sus cimientos para procesar datos evento por evento, en tiempo real y con un control preciso sobre su estado y la noción del tiempo.

Análisis detallado de sus conceptos fundamentales, su arquitectura y los casos de uso que lo han consolidado como un estándar en la industria.

### El Paradigma: Procesamiento de Flujos "Verdadero"

La principal diferencia entre Flink y otros frameworks populares como **Apache Spark Streaming** reside en su modelo de procesamiento fundamental.

| Característica | Apache Flink | Apache Spark Streaming |
| :--- | :--- | :--- |
| **Modelo de Procesamiento** | **Nativo de flujos**: Procesa cada evento individualmente en el momento en que llega. | **Micro-lotes**: Divide el flujo en pequeños lotes de datos para procesarlos por separado. |
| **Latencia** | **Muy baja (milisegundos)**: Ideal para sistemas que requieren una respuesta casi instantánea. | **Media a alta (segundos)**: La latencia mínima está determinada por la duración del micro-lote. |
| **Caso de Uso Típico** |Detección de fraudes, monitoreo de transacciones financieras, IoT en tiempo real. | Análisis de dashboards, ETL con ventanas de tiempo amplias, procesamiento de logs. |
| **Manejo de Estado** |Nativo y potente, con checkpoints incrementales para gestionar terabytes de estado. Gestionado a través del checkpointing de Spark, pero con mayor overhead en recuperaciones. |

Esta arquitectura "nativa" le permite a Flink ofrecer un rendimiento superior en escenarios donde la latencia ultrabaja y el procesamiento de eventos complejos y con estado son críticos.

### Conceptos Fundamentales

Para entender cómo Flink logra este rendimiento, es necesario conocer sus pilares arquitectónicos:

*   **Procesamiento de Flujos con Estado (*Stateful Stream Processing*)**: Una de las características más poderosas de Flink es su capacidad para recordar información a lo largo del tiempo. Cada operación (como una suma o una agregación) puede mantener un "estado" que se actualiza a medida que llegan nuevos eventos. Por ejemplo, para contar cuántas veces ha hecho clic un usuario en los últimos 5 minutos, Flink no necesita consultar una base de datos externa; el recuento se mantiene en el estado local de la aplicación, lo que la hace extremadamente rápida.

*   **Semántica del Tiempo (*Time Semantics*)**: En el procesamiento de flujos, el tiempo es un concepto complejo. Flink maneja tres tipos:
    *   **Tiempo de Procesamiento (*Processing Time*)**: Es la hora del reloj de la máquina que está ejecutando el pipeline. Es simple, pero poco fiable si los datos llegan con retraso.
    *   **Tiempo de Evento (*Event Time*)**: Es la marca de tiempo que el propio dato lleva incrustada (ej: el momento exacto en que un usuario hizo clic). Esta es la forma más precisa de procesar datos, ya que respeta la realidad, incluso si los eventos llegan desordenados al sistema.
    *   **Mecanismo de *Watermarks***: Para gestionar el tiempo de evento y manejar la llegada de datos atrasados, Flink utiliza las **watermarks**. Son marcadores que indican el progreso del tiempo de evento dentro del flujo, permitiendo al sistema saber cuándo es seguro asumir que "no llegarán más eventos de un instante pasado".

*   **Ventanas (*Windows*)**: Es la forma de agrupar un flujo infinito en conjuntos finitos para realizar cálculos (como sumas o promedios). Flink ofrece varios tipos de ventanas muy flexibles:
    *   **Tumbling Windows**: Ventanas de tamaño fijo que no se superponen (ej: cada 5 minutos).
    *   **Sliding Windows**: Ventanas de tamaño fijo que se superponen (ej: una ventana de 10 minutos que se desplaza cada 5 minutos).
    *   **Session Windows**: Ventanas que se agrupan por períodos de inactividad (ej: una sesión de usuario que termina tras 30 minutos sin actividad).

### Arquitectura y APIs

Flink es un sistema distribuido diseñado para ser resiliente y escalar horizontalmente en un clúster de máquinas. Su arquitectura se compone de un **JobManager** (que coordina la ejecución) y uno o más **TaskManagers** (que ejecutan las operaciones sobre los flujos de datos).

Para interactuar con este potente motor, Flink ofrece diferentes niveles de abstracción según la complejidad de la tarea:

*   **DataStream API**: Es la API principal para aplicaciones de streaming. Proporciona un conjunto de operadores ( `map`, `filter`, `keyBy`, etc.) para construir pipelines de forma declarativa, similar a las colecciones en Java o Scala, pero sobre flujos de datos continuos.
*   **ProcessFunction**: Es el nivel más bajo y expresivo. Permite acceder al estado, los temporizadores y las watermarks, dando un control total sobre cada evento que se procesa. Es la base para construir lógicas de detección de patrones complejos (CEP).
*   **Flink SQL / Table API**: Es la interfaz de alto nivel que permite a los analistas y científicos de datos ejecutar consultas SQL estándar directamente sobre los flujos de datos. Compila las consultas a planes de ejecución optimizados en la DataStream API.

### Casos de Uso en la Industria

La capacidad de Flink para manejar grandes volúmenes de datos con una latencia mínima y un estado consistente lo ha convertido en la tecnología elegida por muchas de las empresas más grandes del mundo.

*   **Alibaba**: Utiliza Flink para procesar el aluvión de transacciones durante el Día del Soltero (Singles' Day), procesando billones de eventos en tiempo real.
*   **Uber y Capital One**: Lo emplean en sus motores de **detección de fraude** y monitoreo de transacciones, donde una decisión en milisegundos puede significar la diferencia entre autorizar o bloquear un pago sospechoso.
*   **Netflix y Spotify**: Lo usan para **recomendaciones en tiempo real** y análisis de la experiencia del usuario, permitiendo ajustar el contenido y la interfaz sobre la marcha.
*   **IoT y Logística**: Empresas como Bouygues Telecom y Comcast procesan miles de millones de eventos de sensores y logs de red al día para monitorizar el estado de su infraestructura y anticiparse a fallos.

**Apache Flink** es la herramienta de referencia para la ingeniería de datos moderna cuando el requisito no es solo manejar el volumen, sino hacerlo con la velocidad y la precisión que exigen las aplicaciones más críticas. Si tu proyecto implica tomar decisiones en tiempo real sobre datos que cambian constantemente, Flink es la opción más sólida del ecosistema actual.

# Instalar Apache Flink 

Guía paso a paso para instalar **Apache Flink** en Linux (modo standalone para desarrollo):

---

## Instalación limpia con Distrobox

Distrobox permite instalar Flink y PyFlink dentro de un contenedor integrado con la terminal del sistema, sin mezclar sus dependencias con la instalación de Python del host.

### Crear el contenedor

Estos comandos se ejecutan en el sistema anfitrión. Se utiliza Ubuntu 22.04 para disponer de Java 11 y Python 3.10:
```bash
sudo apt update
sudo apt install podman distrobox -y
distrobox create --name flink-dev --image ubuntu:22.04
distrobox enter flink-dev
```

Los pasos restantes se ejecutan dentro de `flink-dev`.

### **1. Instalar dependencias dentro del contenedor**
```bash
sudo apt update
sudo apt install openjdk-11-jdk python3 python3-pip python3-venv wget -y

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export FLINK_PYTHON_ENV="$HOME/flink-venv"

python3 -m venv "$FLINK_PYTHON_ENV"
"$FLINK_PYTHON_ENV/bin/python" -m pip install --upgrade pip setuptools wheel
"$FLINK_PYTHON_ENV/bin/python" -m pip install apache-flink
```

Para conservar las variables después de salir y volver a entrar al contenedor:
```bash
cat >> ~/.bashrc <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export FLINK_HOME=$HOME/flink
export FLINK_PYTHON_ENV=$HOME/flink-venv
export PATH=$PATH:$FLINK_HOME/bin
EOF
source ~/.bashrc
```

Continuar con los pasos **2 a 8** de esta guía. En el paso 2, Flink quedará instalado en `$HOME/flink` en lugar de `/opt/flink`; por tanto, utiliza:
```bash
tar -xzf "$FLINK_PACKAGE"
mv "flink-${FLINK_VERSION}" "$FLINK_HOME"
```

En el paso 8, ejecutar el job con el Python del entorno virtual:
```bash
$FLINK_HOME/bin/flink run \
  -Dpython.client.executable="$FLINK_PYTHON_ENV/bin/python" \
  -py wordcount.py
```

Para volver a entrar al entorno:
```bash
distrobox enter flink-dev
```

Para eliminarlo completamente, desde el sistema anfitrión:
```bash
distrobox stop flink-dev
distrobox rm flink-dev
```

---

### **1. Prerrequisitos**

#### **Instalar Java** (Flink requiere Java 8 u 11):
```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
```

#### **Verificar Java**:
```bash
java -version
```

#### **Configurar JAVA_HOME**:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

#### **Instalar Python y PyFlink**:
```bash
sudo apt install python3 python3-pip python3-venv -y
python3 --version

# Crear un entorno aislado para PyFlink
python3 -m venv "$HOME/flink-venv"
"$HOME/flink-venv/bin/python" -m pip install --upgrade pip setuptools wheel
"$HOME/flink-venv/bin/python" -m pip install apache-flink
```

---

### **2. Descargar e instalar Apache Flink**

#### **Descargar Flink**:
```bash
cd ~

# Obtener la última versión binaria publicada por Apache Flink
FLINK_VERSION="$(wget -qO- https://downloads.apache.org/flink/ \
  | grep -oE 'href="flink-[0-9]+\.[0-9]+\.[0-9]+/"' \
  | sed -E 's/.*flink-([0-9.]+)\/.*/\1/' \
  | sort -V \
  | tail -n 1)"

test -n "$FLINK_VERSION" || {
  echo "No se pudo determinar la última versión de Flink"
  exit 1
}

FLINK_PACKAGE="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
wget "https://downloads.apache.org/flink/flink-${FLINK_VERSION}/${FLINK_PACKAGE}"
```

#### **Extraer y mover**:
```bash
tar -xzf "$FLINK_PACKAGE"
sudo mv "flink-${FLINK_VERSION}" /opt/flink
```

#### **Configurar permisos**:
```bash
sudo chown -R $USER:$USER /opt/flink
```

---

### **3. Configurar variables de entorno**

Editar `~/.bashrc`:
```bash
nano ~/.bashrc
```

Agregar al final:
```bash
# Flink Environment Variables
export FLINK_HOME=/opt/flink
export PATH=$PATH:$FLINK_HOME/bin
```

Aplicar los cambios:
```bash
source ~/.bashrc
```



---

### **4. Configurar Flink (Opcional)**

#### **Configurar memoria** (editar `conf/flink-conf.yaml`):
```bash
cd $FLINK_HOME
nano conf/flink-conf.yaml
```

Modificar o agregar:
```yaml
# Memoria para JobManager
jobmanager.memory.process.size: 1024m

# Memoria para TaskManager  
taskmanager.memory.process.size: 1024m

# Número de slots por TaskManager
taskmanager.numberOfTaskSlots: 2

# Parallelismo por defecto
parallelism.default: 2
```

---

### **5. Iniciar Flink en modo standalone**

#### **Iniciar cluster**:
```bash
$FLINK_HOME/bin/start-cluster.sh
```

#### **Verificar estado**:
```bash
$FLINK_HOME/bin/flink list
```

#### **Verificar procesos**:
```bash
jps
```
Deberías ver:
```
StandaloneSessionClusterEntrypoint
TaskManagerRunner
```

---

### **6. Acceder a la interfaz web**

Abrir tu navegador en:
```
http://localhost:8081
```

Deberías ver el **Dashboard de Flink** con información del cluster.

---

### **7. Probar Flink con ejemplos**

#### **Ejecutar ejemplo de WordCount**:
```bash
# Enviar job de ejemplo
$FLINK_HOME/bin/flink run $FLINK_HOME/examples/streaming/WordCount.jar

# Con entrada de parámetros
$FLINK_HOME/bin/flink run $FLINK_HOME/examples/streaming/WordCount.jar --input $FLINK_HOME/README.txt --output /tmp/wordcount-output
```

#### **Ver resultados**:
```bash
cat /tmp/wordcount-output
```

---

### **8. Ejecutar un job personalizado**

#### **Crear archivo Python** (para PyFlink):
```bash
cat > wordcount.py << 'EOF'
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common import SimpleStringSchema, WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:///opt/flink/lib/flink-connector-files-1.17.1.jar")

# Crear source
ds = env.from_collection(["hello world", "hello flink", "flink is great"])

# Transformaciones
result = ds.flat_map(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .key_by(lambda x: x[0]) \
    .reduce(lambda a, b: (a[0], a[1] + b[1]))

# Imprimir resultado
result.print()

# Ejecutar
env.execute("WordCount Python")
EOF
```

#### **Ejecutar job Python**:
```bash
$FLINK_HOME/bin/flink run \
  -Dpython.client.executable="$HOME/flink-venv/bin/python" \
  -py wordcount.py
```

---

### **9. Modo sesión interactiva**

#### **Iniciar SQL Client**:
```bash
$FLINK_HOME/bin/sql-client.sh
```

#### **Ejemplos en SQL Client**:
```sql
-- Crear tabla
CREATE TABLE Words (
    word STRING
) WITH (
    'connector' = 'datagen',
    'fields.word.length' = '5'
);

-- Consultar
SELECT word, COUNT(*) as count 
FROM Words 
GROUP BY word;
```

---

### **10. Detener Flink**

```bash
$FLINK_HOME/bin/stop-cluster.sh
```

---

### **Instalación con Docker (Alternativa)**

#### **Usar Docker Compose**:
```bash
mkdir flink-docker && cd flink-docker

cat > docker-compose.yml << 'EOF'
version: "2.2"
services:
  jobmanager:
    image: flink:1.17.1-scala_2.12
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
        
  taskmanager:
    image: flink:1.17.1-scala_2.12
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
EOF

docker-compose up -d
```

---

### **Comandos útiles**

```bash
# Ver jobs ejecutándose
$FLINK_HOME/bin/flink list

# Cancelar job
$FLINK_HOME/bin/flink cancel <jobID>

# Ver logs
tail -f $FLINK_HOME/log/flink-*-standalonesession-*.log

# Configurar más TaskManagers
$FLINK_HOME/bin/taskmanager.sh start
```

---

### **Solución de problemas**

#### **Error: Puerto 8081 ocupado**:
```bash
# Cambiar puerto en flink-conf.yaml
rest.port: 8082
```

#### **Error: Java no encontrado**:
Verificar `JAVA_HOME` en `~/.bashrc`.

#### **Error: Memoria insuficiente**:
Aumentar en `conf/flink-conf.yaml`:
```yaml
taskmanager.memory.process.size: 2048m
jobmanager.memory.process.size: 2048m
```

