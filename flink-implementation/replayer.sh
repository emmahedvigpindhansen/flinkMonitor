#!/bin/bash

# INPUT: flink (which is assumed configured) directory

PROCESSORS=1

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
JARPATH="$WORKDIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
DATADIR="$WORKDIR/evaluation/synthetic"
FORMULA="$DATADIR/triangle-neg.mfotl"
FLINKDIR="/Users/emmahedvigpindhansen/Desktop/BA/flink"

echo "$WORKDIR"

if [[ ! -z $2 ]]; then

    FORMULA=$2

fi

if [[ ! -r $JARPATH ]]; then
    echo "Error: Could not find monitor jar: $JARPATH"
    exit 2
fi

#FLINKDIR="$1"
#if [[ -z $FLINKDIR || ! -x $FLINKDIR/bin/flink ]]; then
#    echo "Usage: $0 <path to Flink> [<path to a formula>]"
#    echo "Make sure that the scripts in the <path to Flink>/bin are executable"
#    exit 2
#fi

TEMPDIR="$(mktemp -d)"
trap 'rm -rf "$TEMPDIR"' EXIT

fail() {
    echo "=== Test failed ==="
    exit 1
}

echo "Testing formula: "
cat $FORMULA

echo "Generating log ..."
"$WORKDIR/generator.sh" -T -e 1000 -i 10 -x 1 60 > "$WORKDIR/trace.csv" && \
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

echo "Running Flink monitor ..."
printf '>end<\n' >> "$TEMPDIR/trace.csv"

"$WORKDIR/replayer.sh" -i csv -f csv -a 0 -o localhost:10101 "$WORKDIR/trace.csv" > "$WORKDIR/trace.log" &
"$FLINKDIR/bin/flink" run "$JARPATH" --in localhost:10101 --format csv \
        --sig "$DATADIR/synth.sig" --formula $FORMULA \
        --negate false --monitor monpoly --command "monpoly -nonewlastts" \
        --processors $PROCESSORS --multi 1 --skipreorder --nparts 1 \
        --out "$WORKDIR/flink-out" --job "integration-test-$RANDOM"
if [[ $? != 0 ]]; then
    fail
fi
find "$WORKDIR/flink-out" -type f -exec cat \{\} + > "$WORKDIR/out.txt"

#echo
#if "$WORKDIR/tests/verdicts_diff.py" "$WORKDIR/reference.txt" "$WORKDIR/out.txt"; then
#    echo "=== Test passed ==="
#else
#    fail
#fi

