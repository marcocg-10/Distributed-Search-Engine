# Petulis Search

Motor de búsqueda distribuido sobre artículos de Wikipedia usando **Apache Spar**k, **MapReduce** y **TF-IDF**.

## Descripción general

**Petulis Search** es un motor de búsqueda académico desarrollado para demostrar los principios de infraestructura distribuida, procesamiento masivo de datos y recuperación de información. El sistema procesa artículos de Wikipedia, construye un índice invertido basado en TF-IDF y permite realizar consultas de búsqueda ordenando los documentos por relevancia.

El proyecto utiliza **Apache Spark con PySpark** para simular una infraestructura distribuida en modo local mediante `local[*]`. Esto permite ejecutar el proyecto en una laptop o en WSL, pero manteniendo el modelo de procesamiento distribuido por particiones.

## Dataset utilizado

El dataset utilizado es: [**Plain Text Wikipedia 2020-11**](https://www.kaggle.com/datasets/ltcmdrdata/plain-text-wikipedia-202011?select=enwiki20201020). Contiene artículos de Wikipedia ya convertidos a texto plano, lo cual permite enfocarse en el procesamiento distribuido y no en la limpieza compleja de archivos XML. Cada archivo tiene una lista de artículos en formato JSON. Cada artículo tiene una estructura similar a la siguiente:

```json
{
  "id": "7751000",
  "title": "M-137 (Michigan highway)",
  "text": "M-137 was a state trunkline highway..."
}
```

## Estructura del proyecto

```text
petulis-search/
│
├── README.md
│
├── src/
│   ├── common.py
│   ├── build_index.py
│   └── search.py
│
├── data/
│   ├── raw/
│   │   └── archivos_json_de_wikipedia/
│   │
│   ├── sample/
│   │   └── muestra_de_archivos_json/
│   │
│   └── index/
│
└── conf/
    └── log4j2.properties
```

## Descripción archivo importantes

### `src/common.py`

Contiene funciones comunes de preprocesamiento de texto. Se encarga de:

* Convertir texto a minúsculas.
* Eliminar acentos.
* Extraer palabras válidas usando expresiones regulares.
* Eliminar stopwords.
* Tokenizar documentos y consultas.

### `src/build_index.py`

Construye el índice invertido TF-IDF. Este archivo realiza la parte pesada del proyecto:

1. Lee los artículos de Wikipedia.
2. Limpia y tokeniza el texto.
3. Calcula TF.
4. Calcula DF.
5. Calcula IDF.
6. Calcula TF-IDF.
7. Construye el índice invertido.
8. Guarda el índice en disco.

### `src/search.py`

Permite consultar el índice previamente construido. No vuelve a procesar Wikipedia. Solamente:

1. Lee el índice invertido.
2. Tokeniza la consulta del usuario.
3. Busca los términos en el índice.
4. Agrupa resultados por documento.
5. Calcula un score de relevancia.
6. Muestra los documentos más relevantes.

### `data/raw/`

Carpeta donde se colocan los archivos originales del dataset de Wikipedia Spark puede leer todos los archivos dentro de esta carpeta.

### `data/index/`

Carpeta donde se guarda el índice generado. Después de ejecutar `build_index.py`, esta carpeta contiene:

```text
data/index/
├── postings/
├── documents/
└── stats.json
```

### `data/index/postings/`

Contiene el índice invertido. Cada fila representa la relación entre un término y un documento:

```text
term | doc_id | tf | idf | tfidf | bucket
```

Ejemplo:

```text
intelligence | 465385 | 0.15 | 7.3 | 1.10 | i
```

## Requisitos en WSL

Este proyecto fue probado usando WSL con Ubuntu. Se recomienda tener:

* WSL instalado.
* Ubuntu en WSL.
* Python 3.
* Java 17.
* Entorno virtual de Python.
* PySpark.

## Instalación en WSL

### 1. Actualizar paquetes

```bash
sudo apt update
sudo apt upgrade -y
```

### 2. Instalar Python, pip y entorno virtual

```bash
sudo apt install -y python3 python3-pip python3-venv
```

### 3. Instalar Java 17

Spark necesita Java para ejecutarse.

```bash
sudo apt install -y openjdk-17-jdk
```

### 4. Crear entorno virtual

Desde la carpeta raíz del proyecto:

```bash
python3 -m venv .venv
```

Activar el entorno:

```bash
source .venv/bin/activate
```

### 5. Instalar dependencias

```bash
pip install pyspark
```

## Preparación del dataset

El dataset está dividido en varios archivos JSON. Cada archivo puede pesar aproximadamente 40 MB. No es necesario unir los archivos. Spark puede leer una carpeta completa.

Ejemplo:

```bash
ls data/raw
```

## Comando para crear el índice

Este comando construye el índice usando los archivos JSON ubicados en `data/raw/`.

```bash
spark-submit --master "local[*]" \
  --py-files src/common.py \
  src/build_index.py \
  --input data/raw/ \
  --output data/index/ \
  --format json \
  --multiline-json \
  --id-col id \
  --title-col title \
  --text-col text \
  --partitions 8 \
  --min-df 2 \
  --overwrite
```

## Comando para buscar

Ejemplo de búsqueda:

```bash
spark-submit --master "local[*]" \
  --py-files src/common.py \
  src/search.py \
  --index data/index/ \
  --query "artificial intelligence machine learning" \
  --top 10
```

Otro ejemplo:

```bash
spark-submit --master "local[*]" \
  --py-files src/common.py \
  src/search.py \
  --index data/index/ \
  --query "distributed systems mapreduce" \
  --top 10
```

Otro ejemplo:

```bash
spark-submit --master "local[*]" \
  --py-files src/common.py \
  src/search.py \
  --index data/index/ \
  --query "world war baseball aircraft" \
  --top 10
```

## ¿Cómo funciona el motor de búsqueda?

El motor tiene dos fases principales:

### Fase 1: Construcción del índice

La construcción del índice ocurre en `build_index.py`. Spark lee los documentos y los reparte en particiones. Luego cada documento se procesa para obtener términos limpios. Ejemplo de documento:

```text
Artificial intelligence is intelligence demonstrated by machines.
```

Después de tokenizar:

```text
artificial
intelligence
intelligence
demonstrated
machines
```

Luego se calculan las frecuencias y se construye el índice invertido.

### Fase 2: Búsqueda

La búsqueda ocurre en `search.py`. Ejemplo de consulta:

```text
artificial intelligence machine learning
```

La consulta se tokeniza:

```text
artificial
intelligence
machine
learning
```

Luego el sistema busca esos términos en el índice invertido, agrupa los resultados por documento y calcula un score. El resultado final se ordena por relevancia.

## Preprocesamiento de texto

El preprocesamiento ocurre en `common.py`.

Este proceso incluye:

1. Convertir texto a minúsculas.
2. Eliminar acentos.
3. Extraer palabras válidas.
4. Eliminar stopwords.
5. Devolver tokens limpios.

Ejemplo:

```text
"Artificial Intelligence and Machine Learning"
```

Después del preprocesamiento:

```text
["artificial", "intelligence", "machine", "learning"]
```

La palabra `"and"` se elimina porque es una stopword.

### Stopwords

Las stopwords son palabras muy frecuentes que normalmente no aportan valor para diferenciar documentos. Ejemplos:

```text
the
and
of
in
is
to
```

## ¿Qué es TF-IDF?

TF-IDF es una medida estadística usada para estimar la importancia de una palabra dentro de un documento en relación con una colección completa.

TF-IDF combina dos componentes:

1. **TF**: Term Frequency.
2. **IDF**: Inverse Document Frequency.

### Cálculo de TF

TF mide qué tan frecuente es un término dentro de un documento. La fórmula usada es:

$$
TF = \frac{cantidad\ de\ veces\ que\ aparece\ el\ término\ en\ el\ documento}{cantidad\ total\ de\ términos\ del\ documento}
$$

**Ejemplo:**

Documento:

```text
machine learning machine
```

Tokens:

```text
machine
learning
machine
```

Cálculo:

$$
TF(machine) = \frac{2}{3} = 0.666
$$

$$
TF(learning) = \frac{1}{3} = 0.333
$$

### Cálculo de DF

DF significa **Document Frequency**. Indica en cuántos documentos aparece un término.

Ejemplo:

```text
machine aparece en 150 documentos
learning aparece en 90 documentos
intelligence aparece en 300 documentos
```

Entonces:

```text
DF(machine) = 150
DF(learning) = 90
DF(intelligence) = 300
```

### Cálculo de IDF

IDF mide qué tan especial o discriminativo es un término dentro de toda la colección. La fórmula usada es:

$$
IDF = log(\frac{N + 1}{DF+1}) + 1
$$

Donde:
* **N**  = total de documentos
* **DF** = cantidad de documentos donde aparece el término

Se usa `+1` para suavizar el cálculo y evitar problemas con divisiones.

Interpretación:

* Si una palabra aparece en muchos documentos, su IDF baja.
* Si una palabra aparece en pocos documentos, su IDF sube.

### Cálculo de TF-IDF

La fórmula final es:

$$
TF\text{-}IDF = TF × IDF
$$

Un término tendrá un peso alto si:

1. Aparece varias veces en un documento.
2. No aparece en demasiados documentos de la colección.

## Score de búsqueda

Cuando se realiza una consulta, el sistema busca los términos de esa consulta en el índice. Luego agrupa por documento y calcula:

```text
score(documento) = suma de los TF-IDF de los términos encontrados
```

**Ejemplo:**

Consulta:

```text
artificial intelligence machine learning
```

Documento A:

```text
artificial -> 0.20
intelligence -> 0.40
machine -> 0.15
learning -> 0.10
```

Score:

```text
0.20 + 0.40 + 0.15 + 0.10 = 0.85
```

El score no es un porcentaje ni una probabilidad. Es una medida relativa de relevancia. Un documento con score más alto se considera más relevante para la consulta.

---

## ¿Cómo se usa MapReduce?

### Fase Map

En la fase Map, cada documento se transforma en pares clave-valor.

Entrada:

```text
Documento de Wikipedia
```

Salida:

```text
((term, doc_id), tf)
```

**Ejemplo:**

Documento:

```text
machine learning machine
```

Salida:

```text
(("machine", "doc1"), 0.666)
(("learning", "doc1"), 0.333)
```

En el código, esta fase ocurre principalmente en:

```python
.flatMap(document_to_tf_pairs)
```

---

### Fase Reduce

En la fase Reduce, Spark agrupa valores con la misma clave.

**Ejemplo:**

```text
(("machine", "doc1"), 0.30)
(("machine", "doc1"), 0.36)
```

Después del reduce:

```text
(("machine", "doc1"), 0.66)
```

En el código, esto ocurre con:

```python
.reduceByKey(lambda a, b: a + b)
```

---

### Segundo Reduce: DF

Luego se calcula cuántos documentos contienen cada término.

Entrada:

```text
((term, doc_id), tf)
```

Transformación:

```text
(term, 1)
```

Reduce:

```text
(term, cantidad_de_documentos)
```

**Ejemplo:**

```text
machine -> 150
learning -> 90
```

## ¿Cómo se minimiza el costo de comunicación?

En sistemas distribuidos, una parte costosa es mover datos entre nodos. Esto ocurre especialmente durante operaciones de shuffle. El proyecto reduce ese costo mediante varias estrategias:

* **Tokenización local:** Cada documento se limpia y tokeniza localmente en su partición. Esto evita mover el texto completo innecesariamente.


* **Uso de `Counter`:** Antes de emitir pares clave-valor, se cuentan las palabras dentro del documento. Esto reduce la cantidad de datos generados.

* **Eliminación de stopwords:** Palabras como `the`, `and`, `of`, `in` aparecen demasiado.

* **Uso de `reduceByKey`:** `reduceByKey` es preferible a `groupByKey` porque permite combinar datos antes de enviarlos completamente por la red. Esto reduce el tráfico durante el shuffle.

* **Filtro `min_df`:** El parámetro: `--min-df 2` elimina términos que aparecen en muy pocos documentos. Esto ayuda a reducir ruido y tamaño del índice.

* **Filtro `max_df_ratio`:** El parámetro: `--max-df-ratio 0.80` elimina términos que aparecen en más del 80% de documentos. Estos términos no ayudan mucho a diferenciar documentos.

* **Buckets:** El índice agrega una columna `bucket`, basada en la primera letra del término. Durante la búsqueda se filtran solo los buckets necesarios. Esto evita revisar todo el índice completo.

## Salida esperada

Una búsqueda puede generar resultados como:

```text
Consulta original: artificial intelligence machine learning
Términos usados: ['artificial', 'intelligence', 'machine', 'learning']
Documentos en el índice: 56315

Resultados:
--------------------------------------------------------------------------------
1. Security Intelligence Service
   doc_id: 465385
   score: 1.103409
   términos encontrados: intelligence
--------------------------------------------------------------------------------
2. Prototype methods
   doc_id: 60590911
   score: 0.523375
   términos encontrados: machine, learning
--------------------------------------------------------------------------------
3. Evolutionary programming
   doc_id: 460689
   score: 0.403873
   términos encontrados: artificial, machine, intelligence, learning
```

## Interpretación del score

El score representa la suma de los valores TF-IDF de los términos encontrados en un documento.
* No representa un porcentaje.
* No representa una probabilidad.

Un score más alto significa que, según el modelo TF-IDF, ese documento tiene mayor relevancia para la consulta. Por ejemplo el score de `1.103409` significa que la suma de los pesos TF-IDF de los términos encontrados en ese documento fue `1.103409`.

## Comando completo recomendado

Crear índice:

```bash
spark-submit --master "local[*]" \
  --conf spark.ui.showConsoleProgress=false \
  --py-files src/common.py \
  src/build_index.py \
  --input data/raw/ \
  --output data/index/ \
  --format json \
  --multiline-json \
  --id-col id \
  --title-col title \
  --text-col text \
  --partitions 8 \
  --min-df 2 \
  --overwrite
```

Buscar:

```bash
spark-submit --master "local[*]" \
  --conf spark.ui.showConsoleProgress=false \
  --py-files src/common.py \
  src/search.py \
  --index data/index/ \
  --query "artificial intelligence machine learning" \
  --top 10
```
