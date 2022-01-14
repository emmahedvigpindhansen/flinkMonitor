#!/bin/bash

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH_BB="$WORKDIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
JARPATH_WB="$WORKDIR/flink-implementation/target/flink-implementation-1.0-SNAPSHOT.jar"
MONPOLY_DIR="/Users/emmahedvigpindhansen/Desktop/BA/monpoly"
DATADIR="$WORKDIR/flink-implementation-tests"
FORMULA="$DATADIR/mformulas/random_mformula.mfotl"
SIGFILE="$DATADIR/sigs/random_mformula.sig"
TRACEFILE="$DATADIR/logs/trace.csv" ##change to random_trace when using random mformula
LOGFILE="$DATADIR/logs/trace.log"
PORT=10105

MFORMULA_SIZE=3
MFORMULA_FREEVARS=2
PROCESSORS=2

fail() {
    echo "=== Test failed ==="
    exit 1
}

echo "Generating formula..."

$MONPOLY_DIR/_build/install/default/bin/gen_fma -size $MFORMULA_SIZE -free_vars $MFORMULA_FREEVARS -output "$DATADIR/mformulas/random_mformula"

cat $FORMULA

echo "Generating log..."

"$WORKDIR/generator.sh" -sig "$SIGFILE" -T -e 10 -i 10 -x 1 20 > "$TRACEFILE" &&
    "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 "$TRACEFILE" > "$LOGFILE"
if [[ $? != 0 ]]; then
    fail
fi

echo "Creating MonPoly reference output ..." # This might be empty
# -nonewlastts removed bc eventually
monpoly -sig "$SIGFILE" -formula "$FORMULA" -log "$LOGFILE" -no_rw > "$WORKDIR/reference.txt"
if [[ $? != 0 ]]; then
    fail
fi
head "$WORKDIR/reference.txt" # check reference output not empty

echo "Running white box monitor ..."
"$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$TRACEFILE" &
scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$FORMULA" \
    --processors "$PROCESSORS" --out "$WORKDIR/flink-out" --format monpoly --job "monpoly-test"
if [[ $? != 0 ]]; then
    fail
fi
find "$WORKDIR/flink-out" -type f -exec cat \{\} + > "$WORKDIR/out_wb.txt"

echo "Comparing output with reference output ..."
if "$WORKDIR/verdicts_diff.py" "$WORKDIR/reference.txt" "$WORKDIR/out_wb.txt"; then
    echo "=== Test passed ==="
else
    fail
fi






