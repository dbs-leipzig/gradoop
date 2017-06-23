#!/bin/bash
PATHZ=~
SIZE=100000
~/Downloads/FLINK/flink-1.1.2/bin/flink run -p 1 --class org.gradoop.benchmark.nesting.RVFOverSerializedData target/gradoop-examples-0.3.0-SNAPSHOT.jar --input $PATHZ/$SIZE --csv /Users/vasistas/benchmarks.csv
~/Downloads/FLINK/flink-1.1.2/bin/flink run -p 1 --class org.gradoop.benchmark.nesting.PerformBenchmarkOverSerializedData target/gradoop-examples-0.3.0-SNAPSHOT.jar --input $PATHZ/$SIZE --csv /Users/vasistas/benchmarks.csv
