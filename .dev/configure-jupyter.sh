#!/usr/bin/env bash
# Script to be used in the Dockerfile to actually configure the default kernels

set -euo pipefail

if ! SPARK_MAGIC_PATH="$(pip show sparkmagic | grep -E 'Location:' | awk '{print $NF}')" || [[ -z "$SPARK_MAGIC_PATH" ]]; then
    echo >&2 "ERROR: couldn't find path to sparkmagic"
    exit 1
fi

echo >&2 "INFO: using sparkmagic path: $SPARK_MAGIC_PATH"

(
    set -x
    jupyter-kernelspec install --replace --user --name 'spark-livy-uploads' "${SPARK_MAGIC_PATH}/sparkmagic/kernels/sparkkernel"
    jupyter-kernelspec install --replace --user --name 'pyspark-livy-uploads' "${SPARK_MAGIC_PATH}/sparkmagic/kernels/pysparkkernel"
    jupyter-serverextension enable --sys-prefix --py sparkmagic
)

kernel_pyspark="$(jupyter-kernelspec list | grep ' pyspark-livy-uploads ' | awk '{print $2}')"
kernel_scala="$(jupyter-kernelspec list | grep ' spark-livy-uploads ' | awk '{print $2}')"

if [ -z "$kernel_pyspark" ]; then
    echo >&2 "ERROR: couldn't get path to installed Pyspark kernel"
    jupyter-kernelspec list >&2
    exit 1
elif ! [ -f "$kernel_pyspark/kernel.json" ]; then
    echo >&2 "ERROR: no kernel.json in Pyspark kernel at '$kernel_pyspark'"
    jupyter-kernelspec list >&2
    exit 1
fi

if [ -z "$kernel_scala" ]; then
    echo >&2 "ERROR: couldn't get path to installed Spark kernel"
    jupyter-kernelspec list >&2
    exit 1
elif ! [ -f "$kernel_scala/kernel.json" ]; then
    echo >&2 "ERROR: no kernel.json in Scala kernel at '$kernel_scala/kernel.json'"
    jupyter-kernelspec list >&2
    exit 1
fi

BASEDIR="$(pwd)"
JQ_EXPR='.argv = (["uv", "--project", $basedir, "run", "sparkrl", "run"] + .argv) | .display_name = $dname'

set -x
touch .env
jq \
    --arg basedir "$BASEDIR" \
    --arg dname "PySpark (Sparkrl)" \
    "$JQ_EXPR" \
    "$kernel_pyspark/kernel.json" | tee "$kernel_pyspark/.kernel.json.tmp"
jq \
    --arg basedir "$BASEDIR" \
    --arg dname "Spark (Sparkrl)" \
    "$JQ_EXPR" \
    "$kernel_scala/kernel.json" | tee "$kernel_scala/.kernel.json.tmp"

mv "$kernel_pyspark/.kernel.json.tmp" "$kernel_pyspark/kernel.json"
mv "$kernel_scala/.kernel.json.tmp" "$kernel_scala/kernel.json"
