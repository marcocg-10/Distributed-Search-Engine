import argparse
import json
import math
import sys
from collections import Counter
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import tokenize

def parse_args():
    """
    Define los argumentos que el programa recibe desde la terminal.

    Ejemplo:

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
    """

    parser = argparse.ArgumentParser(
        description="Construye el índice invertido TF-IDF de Petulis Search."
    )

    parser.add_argument("--input", required=True, help="Ruta del dataset.")
    parser.add_argument("--output", required=True, help="Ruta de salida del índice.")
    parser.add_argument("--format", choices=["jsonl", "json"], default="jsonl", help="Formato del dataset.")
    parser.add_argument("--multiline-json", action="store_true", help="Usar si cada archivo JSON contiene un arreglo de artículos.")
    parser.add_argument("--id-col", default=None, help="Columna de ID.")
    parser.add_argument("--title-col", default=None, help="Columna de título.")
    parser.add_argument("--text-col", default=None, help="Columna de texto.")
    parser.add_argument("--max-docs", type=int, default=0, help="Máximo de documentos a procesar. 0 significa todos.")
    parser.add_argument("--partitions", type=int, default=8, help="Número de particiones Spark.")
    parser.add_argument("--min-df", type=int, default=1, help="Frecuencia documental mínima.")
    parser.add_argument("--max-df-ratio", type=float, default=0.80, help="Porcentaje máximo de documentos donde puede aparecer un término.")
    parser.add_argument("--overwrite", action="store_true", help="Sobrescribe el índice si ya existe.")
    parser.add_argument("--show-schema", action="store_true", help="Muestra el esquema detectado del dataset y termina.")

    # Devuelve los argumentos ya procesados.
    return parser.parse_args()

def create_spark() -> SparkSession:
    """
    Crea la sesión de Spark.

    SparkSession es el punto de entrada para usar Spark. Se configura:
    - Nombre de la aplicación.
    - Ocultar barra de progreso.
    - Número de particiones para operaciones de shuffle.
    """

    return (
        SparkSession.builder
        .appName("PetulisBuildIndex")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def pick_column(columns, preferred, candidates):
    """
    Escoge una columna del dataset.

    Primero intenta usar la columna indicada por el usuario.
    Si el usuario no indicó una, busca entre nombres comunes.

    Ejemplo:
    Para texto puede buscar:
    text, content, body, article, document.
    """

    if preferred:
        if preferred not in columns:
            raise ValueError(f"La columna indicada no existe: {preferred}")
        return preferred

    for candidate in candidates:
        if candidate in columns:
            return candidate

    return None

def load_documents_df(spark: SparkSession, args):
    """
    Carga los documentos desde el dataset.

    El resultado final siempre tendrá esta forma:

    doc_id | title | text
    """

    reader = spark.read.option("recursiveFileLookup", "true")

    # Si el JSON es multilineal, entonces Spark necesita multiLine=true.
    if args.format == "json":
        reader = reader.option("multiLine", str(args.multiline_json).lower())

    df = reader.json(args.input)

    # Permite inspeccionar columnas antes de procesar.
    if args.show_schema:
        df.printSchema()
        print("Columnas detectadas:", df.columns)
        sys.exit(0)

    columns = df.columns

    id_col = pick_column(
        columns,
        args.id_col,
        ["id", "doc_id", "page_id", "article_id", "url"],
    )

    title_col = pick_column(
        columns,
        args.title_col,
        ["title", "name", "article_title"],
    )

    text_col = pick_column(
        columns,
        args.text_col,
        ["text", "content", "body", "article", "document"],
    )

    if text_col is None:
        raise ValueError(
            "No pude detectar la columna de texto. "
            "Ejecutá con --show-schema y luego indicá --text-col."
        )

    # Si no existe columna de ID, Spark genera una.
    doc_id_expr = (
        F.col(id_col).cast("string")
        if id_col
        else F.monotonically_increasing_id().cast("string")
    )

    # Si no existe título, se genera uno básico.
    title_expr = (
        F.col(title_col).cast("string")
        if title_col
        else F.concat(F.lit("doc-"), doc_id_expr)
    )

    docs = df.select(
        doc_id_expr.alias("doc_id"),
        title_expr.alias("title"),
        F.col(text_col).cast("string").alias("text"),
    )

    docs = docs.filter(F.col("text").isNotNull())

    if args.max_docs > 0:
        docs = docs.limit(args.max_docs)

    return docs

def document_to_tf_pairs(row):
    """
    MAP principal del proyecto.

    Entrada:
        Un documento con:
        - doc_id
        - title
        - text

    Salida:
        Lista de pares:

        ((term, doc_id), tf)

    Ejemplo:

        Documento doc1:
        "machine learning machine"

        Tokens:
        ["machine", "learning", "machine"]

        Frecuencias:
        machine = 2
        learning = 1

        TF:
        machine = 2 / 3
        learning = 1 / 3

        Salida:
        (("machine", "doc1"), 0.666)
        (("learning", "doc1"), 0.333)
    """

    doc_id = row["doc_id"]
    tokens = tokenize(row["text"])

    if not tokens:
        return []

    counts = Counter(tokens)
    total_terms = len(tokens)

    return [
        ((term, doc_id), count / total_terms)
        for term, count in counts.items()
    ]

def main():
    """
    Construye el índice invertido TF-IDF.
    """

    args = parse_args()

    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = Path(args.output)
    mode = "overwrite" if args.overwrite else "error"

    # Carga documentos y los reparte en particiones.
    docs_df = load_documents_df(spark, args).repartition(args.partitions)
    docs_df.cache()

    total_docs = docs_df.count()

    if total_docs == 0:
        raise ValueError("No se encontraron documentos para procesar.")

    print(f"Documentos procesados: {total_docs}")
    print(f"Particiones usadas: {docs_df.rdd.getNumPartitions()}")

    # Guarda únicamente la metadata de documentos.
    # Esto permite mostrar títulos en las búsquedas.
    docs_meta_df = docs_df.select("doc_id", "title")

    # MAP + REDUCE 1: cálculo de TF
    #
    # MAP:
    # documento -> ((term, doc_id), tf)
    #
    # REDUCE:
    # agrupa por (term, doc_id)
    term_doc_tf_rdd = (
        docs_df.rdd
        .flatMap(document_to_tf_pairs)
        .reduceByKey(lambda a, b: a + b, numPartitions=args.partitions)
    )

    # REDUCE 2: cálculo de DF
    #
    # DF = Document Frequency.
    #
    # Indica en cuántos documentos aparece cada término.
    #
    # Entrada:
    # ((term, doc_id), tf)
    #
    # Salida:
    # (term, cantidad_de_documentos)
    term_df_rdd = (
        term_doc_tf_rdd
        .map(lambda item: (item[0][0], 1))
        .reduceByKey(lambda a, b: a + b, numPartitions=args.partitions)
    )

    # Se descartan términos demasiado comunes.
    # Por ejemplo, si un término aparece en más del 80% de documentos,
    # aporta poca relevancia para diferenciar resultados.
    max_df = int(total_docs * args.max_df_ratio)

    filtered_df_rdd = term_df_rdd.filter(
        lambda item: args.min_df <= item[1] <= max_df
    )

    # Cálculo de IDF
    #
    # Fórmula:
    # idf = log((N + 1) / (df + 1)) + 1
    #
    # N  = total de documentos
    # df = cantidad de documentos donde aparece el término
    term_idf_rdd = filtered_df_rdd.mapValues(
        lambda df: math.log((total_docs + 1) / (df + 1)) + 1
    )

    # Join entre TF e IDF
    #
    # Se reorganiza el RDD para que la clave sea el término.
    #
    # Antes:
    # ((term, doc_id), tf)
    #
    # Después:
    # (term, (doc_id, tf))
    tf_by_term_rdd = term_doc_tf_rdd.map(
        lambda item: (item[0][0], (item[0][1], item[1]))
    )

    # Se une cada TF con el IDF correspondiente.
    #
    # Resultado final:
    # term, doc_id, tf, idf, tfidf
    tfidf_rdd = (
        tf_by_term_rdd
        .join(term_idf_rdd, numPartitions=args.partitions)
        .map(
            lambda item: (
                item[0],                 # term
                item[1][0][0],           # doc_id
                float(item[1][0][1]),    # tf
                float(item[1][1]),       # idf
                float(item[1][0][1] * item[1][1]),  # tfidf
            )
        )
    )

    # Esquema del índice invertido.
    schema = StructType([
        StructField("term", StringType(), False),
        StructField("doc_id", StringType(), False),
        StructField("tf", DoubleType(), False),
        StructField("idf", DoubleType(), False),
        StructField("tfidf", DoubleType(), False),
    ])

    postings_df = spark.createDataFrame(tfidf_rdd, schema)

    # Bucket basado en la primera letra del término.
    #
    # Esto permite que search.py filtre por bucket
    # y no tenga que revisar todo el índice completo.
    postings_df = postings_df.withColumn(
        "bucket",
        F.substring(F.col("term"), 1, 1),
    )

    postings_output = str(output_path / "postings")
    docs_output = str(output_path / "documents")

    # Guarda el índice invertido.
    postings_df.write.mode(mode).json(postings_output)

    # Guarda metadata de documentos.
    docs_meta_df.write.mode(mode).json(docs_output)

    # Guarda estadísticas útiles para mostrar durante la búsqueda.
    stats = {
        "total_docs": total_docs,
        "partitions": args.partitions,
        "min_df": args.min_df,
        "max_df_ratio": args.max_df_ratio,
        "description": "Petulis Search TF-IDF inverted index",
    }

    output_path.mkdir(parents=True, exist_ok=True)

    with open(output_path / "stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=4)

    print("Índice construido correctamente.")
    print(f"Postings: {postings_output}")
    print(f"Documentos: {docs_output}")
    print(f"Stats: {output_path / 'stats.json'}")

    spark.stop()

if __name__ == "__main__":
    main()