A task-parallel online monitor for metric first-order temporal logic (MFOTL)
---------

A white-box parallel online monitor implemented on the Apache Flink® distributed stream
processing engine. The implementation code can be found in flink-implementation and tests in flink-implementation-tests.

For information on software requirements see https://bitbucket.org/krle/scalable-online-monitor/src/master/evaluation/setup.sh. Note that this implementation relies on Flink 1.11.2 whereas the scalable online monitor uses 1.7.2.

Required arguments: --out, --sig, --formula, --format

--checkpoints <URI>         If set, checkpoints are saved in the directory at <URI>
                            Example: --checkpoints file:///foo/bar

--in <file>                 Read events from <file> (see also: --watch, default: 127.0.0.1:9000)
--in <host>:<port>          Connect to the given TCP server and read events

--format monpoly|csv        Format of the input events

--watch true|false          If set to true, the argument of --in is interpreted as a directory (default: false).
                            The monitor watches for new files in the directory and reads events from them.

--out <file>                Write verdicts to <file>
--out <host>:<port>         Connect to the given TCP server and write verdicts

--monitor <command>         Name of the monitor executable and additional arguments (default: "monpoly -negate")

--sig <file>                Name of the signature file

--formula <file>            Name of the file with the MFOTL formula

--processors <N>            Number of parallel monitors (default: 1)

--job <name>                Name of the Apache Flink job

Utilities
---------

replayer.sh, generator.sh: See documentation printed by --help.
