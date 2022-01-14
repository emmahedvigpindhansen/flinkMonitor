#!/bin/env bash

WORKDIR=`cd "$(dirname "$BASH_SOURCE")/.."; pwd`
FLINK_11_BIN="/Users/emmahedvigpindhansen/Desktop/BA/flink-1.11.2/bin"
FLINK_7_BIN="/Users/emmahedvigpindhansen/Desktop/BA/flink-1.7.2/bin"
JARPATH_BB="$WORKDIR/flink-monitor/target/flink-monitor-1.0-SNAPSHOT.jar"
JARPATH_WB="$WORKDIR/flink-implementation/target/flink-implementation-1.0-SNAPSHOT.jar"
MONPOLY_DIR="/Users/emmahedvigpindhansen/Desktop/BA/monpoly"
LOG_DIR="$WORKDIR/flink-implementation-tests/logs"
REPORT_DIR="$WORKDIR/flink-implementation-tests/reports"
OUTPUT_DIR="$WORKDIR/flink-implementation-tests/output"
MFORMULA_DIR="$WORKDIR/flink-implementation-tests/mformulas"
SIGFILE="$WORKDIR/flink-implementation-tests/sigs/synth.sig"

FORMULAS="linear-neg star-neg triangle-neg"
NEGATE="" # if formulas above are suffixed with -neg this should be "", otherwise "-negate"
EVENT_RATES="25000 30000 35000 40000 45000 50000"
ACCELERATIONS="0 1"
LOG_LENGTH=60
REPETITIONS=3

PROCESSORS_WB="1 2 3 4"
PROCESSORS_BB="5 9 13 17"

STREAM_PORT=10102

echo "=== Synthetic experiments (relation sizes) ==="

make_log() {
    flag=$1
    formula=$2
    for er in $EVENT_RATES; do
        "$WORKDIR/generator.sh" "$flag" -sig "$SIGFILE" -e "$er" -i 1 -w 10 -pA 0.01 -pB 0.495 $LOG_LENGTH > "$LOG_DIR/gen_${formula}_${er}.csv"
        "$WORKDIR/replayer.sh" -i csv -f monpoly -a 0 "$LOG_DIR/gen_${formula}_${er}.csv" > "$LOG_DIR/gen_${formula}_${er}.log"
    done
}

echo "Generating logs ..."
make_log -S star-neg
make_log -L linear-neg
make_log -T triangle-neg

start_time=$(date +%Y%m%d_%H%M%S)

"$FLINK_11_BIN/start-cluster.sh" > /dev/null

echo "Running white-box tests"
for procs in $PROCESSORS_WB; do
    for formula in $FORMULAS; do
        for er in $EVENT_RATES; do
            for acc in $ACCELERATIONS; do
                for i in $(seq 1 $REPETITIONS); do
                    echo "Evaluating no. processors $procs"
                    echo "Evaluating $formula:"
                    echo "Event rate $er:"
                    echo "Acceleration $acc:"
                    echo "Repetition $i ..."

                    INPUT_FILE="$LOG_DIR/gen_${formula}_${er}.csv"

                    if [[ "$acc" = "0" ]]; then

                        JOB_NAME="gen_flink_wb_${procs}_${formula}_${er}_${i}_offline"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        "$WORKDIR/replayer.sh" -v -a 0 -i csv -f monpoly -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                          (time $FLINK_11_BIN/flink run $JARPATH_WB --in localhost:$STREAM_PORT --format monpoly \
                           --sig $SIGFILE --formula $MFORMULA_DIR/"$formula".mfotl \
                           --negate false --out flink-out --processors "$procs" --job "$JOB_NAME") 2> "$JOB_REPORT"
                    else

                        JOB_NAME="gen_flink_wb_${procs}_${formula}_${er}_${i}_online"
                        DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                        JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                        "$WORKDIR/replayer.sh" -v -a 1 -i csv -f monpoly -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                          (time $FLINK_11_BIN/flink run $JARPATH_WB --in localhost:$STREAM_PORT --format monpoly \
                           --sig $SIGFILE --formula $MFORMULA_DIR/"$formula".mfotl \
                           --negate false --out flink-out --processors "$procs" --job "$JOB_NAME") 2> "$JOB_REPORT"
                    fi
                done
            done
        done
    done
done

"$FLINK_11_BIN/stop-cluster.sh" > /dev/null

"$FLINK_7_BIN/start-cluster.sh" > /dev/null

echo "Running black-box tests"
for procs in $PROCESSORS_BB; do
    for formula in $FORMULAS; do
        for er in $EVENT_RATES; do
            for i in $(seq 1 $REPETITIONS); do
                echo "Evaluating no. processors $procs"
                echo "Evaluating $formula:"
                echo "Event rate $er:"
                echo "Repetition $i ..."

                INPUT_FILE="$LOG_DIR/gen_${formula}_${er}.csv"

                JOB_NAME="gen_flink_bb_${procs}_${formula}_${er}_${i}"
                DELAY_REPORT="$REPORT_DIR/${JOB_NAME}_delay.txt"
                JOB_REPORT="$REPORT_DIR/${JOB_NAME}_job.txt"

                "$WORKDIR/replayer.sh" -v -a 1 -i csv -f monpoly -t 1000 -o localhost:$STREAM_PORT "$INPUT_FILE" 2> "$DELAY_REPORT" &
                (time $FLINK_7_BIN/flink run $JARPATH_BB --skipreorder true --in localhost:$STREAM_PORT --format monpoly \
                   --sig $SIGFILE --formula $MFORMULA_DIR/"$formula".mfotl \
                   --negate false --out flink-out --processors "$procs") 2> "$JOB_REPORT"
            done
        done
    done
done

"$FLINK_7_BIN/stop-cluster.sh" > /dev/null

end_time=$(date +%Y%m%d_%H%M%S)

echo "Scraping metrics from $start_time to $end_time ..."

echo "Evaluation complete!"
