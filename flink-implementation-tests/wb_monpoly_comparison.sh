#!/bin/bash

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH_WB="$WORKDIR/flink-implementation/target/flink-implementation-1.0-SNAPSHOT.jar"
TEST_DIR="$WORKDIR/flink-implementation-tests"
MONPOLY_DIR="/Users/emmahedvigpindhansen/Desktop/BA/monpoly"
LOG_DIR="$WORKDIR/flink-implementation-tests/logs"
MFORMULA_DIR="$WORKDIR/flink-implementation-tests/mformulas"
SIG_DIR="$WORKDIR/flink-implementation-tests/sigs"
OUTPUT_DIR="$WORKDIR/flink-implementation-tests/output"
PORT=10106
PROCESSORS=2

echo "Test script to compare white-box monitor output to MonPoly reference output."

run_monpoly() {
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$1" -no_rw "$2" "$3" > "$OUTPUT_DIR/ref_$formula.txt"
}

run_wb() {
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$1" &
    scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" \
    --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --format monpoly --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/wb_$formula.txt"
}

compare() {
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/ref_$formula.txt" "$OUTPUT_DIR/wb_$formula.txt"; then
    echo "=== Test passed ==="
  else
    echo "=== Test failed ==="
  fi
}

echo "Testing Not binary cases..."
# -nonewlastts removed bc Until
FORMULAS="not_and not_since not_until"
SIGFILE="$SIG_DIR/not.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  run_monpoly "$LOG_DIR/$formula.log"
  run_wb "$LOG_DIR/$formula.csv"
  compare
done

echo "Testing cases with only one operator ..."
FORMULAS="and once or since next prev"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  run_monpoly "$LOG_DIR/rv11.log" -nonewlastts
  run_wb "$LOG_DIR/rv11.csv"
  compare
done

# here we need to add -verified to MonPoly
FORMULAS="exists and_exists pred"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  run_monpoly "$LOG_DIR/rv11.log" -nonewlastts -verified
  run_wb "$LOG_DIR/rv11.csv"
  compare
done

# -nonewlastts removed bc Until/Eventually
FORMULAS="eventually until"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  run_monpoly "$LOG_DIR/rv11.log"
  run_wb "$LOG_DIR/rv11.csv"
  compare
done


echo "Testing cases with multiple operators ..."
FORMULAS="and_once once_and once_and_next once_and_prev or_next or_prev not_since_and"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  run_monpoly "$LOG_DIR/rv11.log" -nonewlastts
  run_wb "$LOG_DIR/rv11.csv"
  compare
done

# -nonewlastts removed bc Until/Eventually
FORMULAS="and_eventually not_until_and once_and_eventually"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  run_monpoly "$LOG_DIR/rv11.log"
  run_wb "$LOG_DIR/rv11.csv"
  compare
done

rm "$OUTPUT_DIR/flink-out"