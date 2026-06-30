#!/bin/bash

mkdir -p results
mkdir -p data/experiments

# PRUEBA 1

echo "Prueba 1: tiempo de indexación según cantidad de documentos"

echo "experiment,value,run_number,time_seconds,size_kb" > results/test_1.csv

for DOCS in 1000 5000 10000 15000 20000 25000 30000 35000 40000
do
    for RUN in {1..5}
    do
        echo "Documentos: $DOCS | Repetición $RUN"

        OUTPUT="data/experiments/index_${DOCS}_run$RUN"

        rm -rf "$OUTPUT"

        START=$(date +%s.%N)

        spark-submit --master "local[*]" \
        --driver-java-options="-Dlog4j.configurationFile=conf/log4j2.properties" \
        --py-files src/common.py \
        src/build_index.py \
        --input data/raw/ \
        --output "$OUTPUT" \
        --format json \
        --multiline-json \
        --id-col id \
        --title-col title \
        --text-col text \
        --max-docs "$DOCS" \
        --partitions 8 \
        --min-df 2 \
        --overwrite 2>/dev/null

        END=$(date +%s.%N)

        TIME=$(echo "$END - $START" | bc)
        SIZE=$(du -sk "$OUTPUT" | cut -f1)

        echo "documents,$DOCS,$RUN,$TIME,$SIZE" >> results/test_1.csv
    done
done

# PRUEBA 2

echo "Prueba 2: tiempo de indexación según cantidad de particiones"

echo "experiment,value,run_number,time_seconds,size_kb" > results/test_2.csv

for PARTITIONS in 2 4 8 16
do
    for RUN in {1..5}
    do
        echo "Particiones: $PARTITIONS | Repetición $RUN"

        OUTPUT="data/experiments/index_p${PARTITIONS}_run${RUN}"

        rm -rf "$OUTPUT"

        START=$(date +%s.%N)

        spark-submit --master "local[*]" \
            --driver-java-options="-Dlog4j.configurationFile=conf/log4j2.properties" \
            --py-files src/common.py \
            src/build_index.py \
            --input data/raw/ \
            --output "$OUTPUT" \
            --format json \
            --multiline-json \
            --id-col id \
            --title-col title \
            --text-col text \
            --max-docs 15000 \
            --partitions "$PARTITIONS" \
            --min-df 2 \
            --overwrite 2>/dev/null

        END=$(date +%s.%N)

        TIME=$(echo "$END - $START" | bc)
        SIZE=$(du -sk "$OUTPUT" | cut -f1)

        echo "partitions,$PARTITIONS,$RUN,$TIME,$SIZE" >> results/test_2.csv
    done
done


# PRUEBA 3
echo "Prueba 3: tiempo de búsqueda"

echo "experiment,value,run_number,time_seconds" > results/test_3.csv

INDEX="data/experiments/index_15000_run1"

if [ ! -d "$INDEX" ]; then
    echo "Error: no existe el índice $INDEX"
    echo "Ejecutá primero la Prueba 1."
    exit 1
fi

QUERIES=(
    "machine learning"
    "artificial intelligence machine"
    "artificial intelligence machine learning distributed systems"
)

for QUERY in "${QUERIES[@]}"
do
    for RUN in {1..5}
    do
        echo "Consulta: \"$QUERY\" | Repetición $RUN"

        START=$(date +%s.%N)

        RESULT=$(spark-submit --master "local[*]" \
            --driver-java-options="-Dlog4j.configurationFile=conf/log4j2.properties" \
            --py-files src/common.py \
            src/search.py \
            --index "$INDEX" \
            --query "$QUERY" \
            --top 10 2>/dev/null)

        END=$(date +%s.%N)

        TIME=$(echo "$END - $START" | bc)
        RESULTS_COUNT=$(echo "$RESULT" | grep -c "doc_id:")

        echo "query,\"$QUERY\",$RUN,$TIME" >> results/test_3.csv
    done
done


echo ""
echo "=========================================="
echo "Resultados guardados en: results "
echo "=========================================="