#!/bin/bash

PROCESSORS=1

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH_BB="$WORKDIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
JARPATH_WB="$WORKDIR/flink-implementation/target/flink-implementation-1.0-SNAPSHOT.jar"
DATADIR="$WORKDIR/evaluation/synthetic"
FORMULA="$DATADIR/simple.mfotl"
FLINKDIR="/Users/emmahedvigpindhansen/Desktop/BA/flink"
PORT=10101 # works with BB but connection refused for WB
# PORT=49194 # this is a free port which leads WB (and BB) to hang as no input is received

echo "$WORKDIR"

fail() {
    echo "=== Test failed ==="
    exit 1
}

echo "Testing formula: "
cat $FORMULA

echo "Generating log ..."
# "$WORKDIR/generator.sh" -T -e 10 -i 10 -x 1 20 > "$WORKDIR/trace.csv" && \
        "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 "$WORKDIR/trace.csv" > "$WORKDIR/trace.log"
if [[ $? != 0 ]]; then
    fail
fi

echo "Creating reference output ..."
monpoly -sig "$DATADIR/synth.sig" -formula $FORMULA -log "$WORKDIR/trace.log" -nonewlastts -no_rw > "$WORKDIR/reference.txt"
if [[ $? != 0 ]]; then
    fail
fi
head n -5 "$WORKDIR/reference.txt"

#echo "Running Flink monitor ..."
#printf '>end<\n' >> "$WORKDIR/trace.csv"

#echo "Running white box monitor ..."
#"$WORKDIR/replayer.sh" -i csv -f csv -a 0 -o localhost:"$PORT" "$WORKDIR/trace.csv" > "$WORKDIR/trace.log" &
#"$FLINKDIR/bin/flink" run "$JARPATH_WB" --in localhost:"$PORT" --format csv \
#        --sig "$DATADIR/synth.sig" --formula $FORMULA \
#        --negate false --monitor monpoly --command "monpoly -nonewlastts" \
#        --processors $PROCESSORS --multi 1 --skipreorder --nparts 1 \
#        --out "$WORKDIR/flink-out" --job "integration-test-$RANDOM"
# if [[ $? != 0 ]]; then
#     fail
# fi
#find "$WORKDIR/flink-out" -type f -exec cat \{\} + > "$WORKDIR/out_wb.txt"

#echo "Running black box monitor ..."
#"$WORKDIR/replayer.sh" -i csv -f csv -a 0 -o localhost:"$PORT" "$WORKDIR/trace.csv" > "$WORKDIR/trace.log" &
#"$FLINKDIR/bin/flink" run "$JARPATH_BB" --in localhost:"$PORT" --format csv \
#        --sig "$DATADIR/synth.sig" --formula $FORMULA \
#        --negate false --monitor monpoly --command "monpoly -nonewlastts" \
#        --processors $PROCESSORS --multi 1 --skipreorder --nparts 1 \
#        --out "$WORKDIR/flink-out" --job "integration-test-$RANDOM"
#if [[ $? != 0 ]]; then
#    fail
#fi
#find "$WORKDIR/flink-out" -type f -exec cat \{\} + > "$WORKDIR/out_bb.txt"

#echo
#if "$WORKDIR/tests/verdicts_diff.py" "$WORKDIR/reference.txt" "$WORKDIR/out.txt"; then
#    echo "=== Test passed ==="
#else
#    fail
#fi

