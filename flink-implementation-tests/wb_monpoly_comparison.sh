#!/bin/bash

PROCESSORS=1

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH_WB="$WORKDIR/flink-implementation/target/flink-implementation-1.0-SNAPSHOT.jar"
TEST_DIR="$WORKDIR/flink-implementation-tests/"
MONPOLY_DIR="/Users/emmahedvigpindhansen/Desktop/BA/monpoly"
LOG_DIR="$WORKDIR/flink-implementation-tests/logs"
MFORMULA_DIR="$WORKDIR/flink-implementation-tests/mformulas"
SIG_DIR="$WORKDIR/flink-implementation-tests/sigs"
OUTPUT_DIR="$WORKDIR/flink-implementation-tests/output"
PORT=10103

echo "Test script to compare white-box monitor output to MonPoly reference output."

echo "Testing Not binary cases..."
# We now test the 'not' binary cases, e.g. Not Since, Not Until, and And Not (what about Not Or!?!?)
# -nonewlastts removed bc Until
FORMULAS="not_and not_since not_until"
SIGFILE="$SIG_DIR/not.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$LOG_DIR/$formula.log" -no_rw > "$OUTPUT_DIR/reference.txt"
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$LOG_DIR/$formula.csv" &
  scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/out_wb.txt"
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/reference.txt" "$OUTPUT_DIR/out_wb.txt"; then
      echo "=== Test passed ==="
  fi
done

echo "Testing cases with publish and approve (only one operator) ..."
FORMULAS="and eventually exists once or since until next prev pred"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$LOG_DIR/rv11.log" -no_rw -nonewlastts > "$OUTPUT_DIR/reference.txt"
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$LOG_DIR/rv11.csv" &
  scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/out_wb.txt"
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/reference.txt" "$OUTPUT_DIR/out_wb.txt"; then
      echo "=== Test passed ==="
  fi
done

echo "Testing cases with publish and approve (multiple operators) ..."
FORMULAS="and_exists and_once or_next or_prev"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$LOG_DIR/rv11.log" -no_rw nonewlastts > "$OUTPUT_DIR/reference.txt"
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$LOG_DIR/rv11.csv" &
  scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/out_wb.txt"
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/reference.txt" "$OUTPUT_DIR/out_wb.txt"; then
      echo "=== Test passed ==="
  fi
done

echo "Testing cases with publish and approve (eventually and until) ..."
# -nonewlastts removed bc Until
FORMULAS="eventually until and_eventually"
SIGFILE="$SIG_DIR/rv11.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$LOG_DIR/rv11.log" -no_rw > "$OUTPUT_DIR/reference.txt"
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$LOG_DIR/rv11.csv" &
  scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/out_wb.txt"
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/reference.txt" "$OUTPUT_DIR/out_wb.txt"; then
      echo "=== Test passed ==="
  fi
done

echo "Testing cases with A, B, C (longer logs) (multiple operators) ..."
FORMULAS="once_and once_and_next once_and_prev"
SIGFILE="$SIG_DIR/synth.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$LOG_DIR/rv11.log" -no_rw -nonewlastts > "$OUTPUT_DIR/reference.txt"
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$LOG_DIR/rv11.csv" &
  scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/out_wb.txt"
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/reference.txt" "$OUTPUT_DIR/out_wb.txt"; then
      echo "=== Test passed ==="
  fi
done

echo "Testing cases with A, B, C (longer logs) (eventually and until) ..."
# -nonewlastts removed bc Until
FORMULAS="once_and_eventually"
SIGFILE="$SIG_DIR/synth.sig"
for formula in $FORMULAS; do
  echo "=== Evaluating $formula ==="
  monpoly -sig "$SIGFILE" -formula "$MFORMULA_DIR/$formula.mfotl" -log "$LOG_DIR/rv11.log" -no_rw > "$OUTPUT_DIR/reference.txt"
  "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 -o localhost:"$PORT" "$LOG_DIR/rv11.csv" &
  scala "$JARPATH_WB" --in localhost:"$PORT" --sig "$SIGFILE" --formula "$MFORMULA_DIR/$formula.mfotl" --processors "$PROCESSORS" --out "$OUTPUT_DIR/flink-out" --job "monpoly-test"
  find "$OUTPUT_DIR/flink-out" -type f -exec cat \{\} + > "$OUTPUT_DIR/out_wb.txt"
  if "$TEST_DIR/verdicts_diff.py" "$OUTPUT_DIR/reference.txt" "$OUTPUT_DIR/out_wb.txt"; then
      echo "=== Test passed ==="
  fi
done





