import argparse
import json
import sys

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import tokenize

def parse_args():
    """
    Esta función define los argumentos que el usuario puede pasar
    cuando ejecuta el programa desde la terminal.

    Ejemplo de uso:

    spark-submit --master "local[*]" src/search.py \
      --index data/index \
      --query "artificial intelligence machine learning" \
      --top 10
    """

    parser = argparse.ArgumentParser(
        description="Consulta el índice TF-IDF de Petulis Search."
    )

    parser.add_argument("--index", required=True, help="Ruta del índice generado por build_index.py.")
    parser.add_argument("--query", required=True, help="Consulta de búsqueda.")
    parser.add_argument("--top", type=int, default=10, help="Cantidad de resultados a mostrar.")

    # Devuelve los argumentos ya procesados.
    return parser.parse_args()

def create_spark() -> SparkSession:
    """
    Crea la sesión de Spark.

    SparkSession es el objeto principal que permite trabajar con Spark.
    En este programa se usa para:
    - leer el índice guardado en JSON
    - filtrar términos
    - agrupar resultados
    - calcular scores
    - ordenar documentos por relevancia
    """

    return (
        SparkSession.builder
        .appName("PetulisSearch")
        .getOrCreate()
    )

def main():
    """
    Función principal del programa.
    """

    # Lee los argumentos recibidos por terminal
    args = parse_args()

    # Crea la sesión de Spark.
    spark = create_spark()

    # Reduce la cantidad de mensajes internos de Spark.
    spark.sparkContext.setLogLevel("WARN")

    index_path = Path(args.index)

    # Ruta donde están los postings del índice invertido.
    #
    # Los postings son las entradas del índice:
    # término -> documento -> tfidf
    #
    # Ejemplo:
    # "intelligence" -> doc_id 123 -> tfidf 0.58
    postings_path = str(index_path / "postings")

    # Ruta donde está la información básica de documentos.
    documents_path = str(index_path / "documents")

    # Ruta del archivo de estadísticas del índice.
    stats_path = index_path / "stats.json"

    # Procesa la consulta del usuario usando la misma función tokenize
    # que se usó durante la construcción del índice.
    #
    # Ejemplo:
    # "Artificial Intelligence Machine Learning"
    #
    # se convierte en:
    # ["artificial", "intelligence", "machine", "learning"]
    query_terms = tokenize(args.query)

    # Elimina términos repetidos manteniendo el orden.
    query_terms = list(dict.fromkeys(query_terms))

    # Si después de limpiar la consulta no queda ningún término válido,
    # el programa termina.
    #
    # Esto puede pasar si el usuario escribe solo palabras vacías como:
    # "the and of"
    if not query_terms:
        print("La consulta no contiene términos válidos.")
        spark.stop()
        return

    # Obtiene la primera letra de cada término.
    #
    # Esto se usa porque el índice tiene una columna llamada "bucket",
    # creada a partir de la primera letra del término.
    #
    # Ejemplo:
    # ["artificial", "intelligence", "machine", "learning"]
    #
    # buckets:
    # ["a", "i", "m", "l"]
    buckets = list({term[0] for term in query_terms if len(term) > 0})

    # Muestra información inicial de la búsqueda.
    print(f"Consulta original: {args.query}")
    print(f"Términos usados: {query_terms}")

    # Si existe el archivo stats.json, lo lee y muestra cuántos documentos
    # fueron indexados.
    if stats_path.exists():
        with open(stats_path, "r", encoding="utf-8") as f:
            stats = json.load(f)

        print(f"Documentos en el índice: {stats.get('total_docs')}")

    # Lee el índice invertido desde disco.
    postings_df = spark.read.json(postings_path)

    # Lee la información de documentos.
    docs_df = spark.read.json(documents_path)

    # Filtra el índice para quedarse solamente con:
    # 1. Los buckets relacionados con la consulta.
    # 2. Los términos exactos de la consulta.
    #
    # Ejemplo:
    # Si la consulta tiene:
    # ["artificial", "intelligence"]
    #
    # solo se buscan términos:
    # artificial
    # intelligence
    #
    # y buckets:
    # a
    # i
    filtered = postings_df.filter(
        (F.col("bucket").isin(buckets)) &
        (F.col("term").isin(query_terms))
    )

    # Se calcula el ranking de documentos.
    ranked = (
        filtered

        # Agrupa todas las coincidencias por documento.
        .groupBy("doc_id")

        # Calcula dos cosas por documento:
        #
        # 1. score:
        #    suma de los valores TF-IDF de los términos encontrados.
        #
        # 2. matched_terms:
        #    conjunto de términos de la consulta que aparecieron
        #    en ese documento.
        .agg(
            F.sum("tfidf").alias("score"),
            F.collect_set("term").alias("matched_terms")
        )

        # Une el ranking con la tabla de documentos para obtener el título.
        .join(docs_df, on="doc_id", how="left")

        # Ordena los documentos de mayor a menor score.
        # Los documentos con mayor TF-IDF aparecen primero.
        .orderBy(F.desc("score"))

        # Limita la cantidad de resultados al valor indicado por --top.
        .limit(args.top)
    )

    # collect() trae los resultados desde Spark hacia Python.
    results = ranked.collect()

    # Si no hay resultados, se informa al usuario.
    if not results:
        print("No se encontraron resultados.")

    else:
        print("\nResultados:")

        # Recorre los resultados y los imprime uno por uno.
        for i, row in enumerate(results, start=1):
            print("-" * 80)

            print(f"{i}. {row['title']}")

            print(f"   doc_id: {row['doc_id']}")

            # Score de relevancia.
            #
            # Este score es la suma de TF-IDF de los términos encontrados.
            # No es porcentaje ni probabilidad.
            # Mientras más alto, más relevante según el modelo.
            print(f"   score: {row['score']:.6f}")

            print(f"   términos encontrados: {', '.join(row['matched_terms'])}")

    spark.stop()

if __name__ == "__main__":
    main()