package org.gradoop.benchmark.fsm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.fsm.common.config.CanonicalLabel;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.GrowthStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.IterationStrategy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TFSMBenchmarkScriptGenerator extends AbstractRunner {

  //-p ${PARALLELISM} -c ${CLASS} ${JAR_FILE} -t${INPUT} ${ARGS}

  private static final String FLINK = "/usr/local/flink-1.1.2/bin/flink run ";
  private static final String BENCHMARK = "b";
  private static final String DATA = "d";

  private static final String PREDICTABLE_PREFIX = "predictable_";
  private static final String HDFS_PATH = "hdfs:///usr/local/hadoop-2.5.2/";
  private static final String YEAST_PREFIX = "yeast_";
  public static final String FOLDER = "/home/hduser/fsm/";

  static {
    OPTIONS.addOption(BENCHMARK, "");
    OPTIONS.addOption(DATA, "");
  }

  private static final int ROUNDS = 1;

  private static final List<String> ROWS = Lists.newArrayList();

  public static final Map<TFSMParam, String> DEFAULTS = Maps.newHashMap();


  static {
    DEFAULTS.put(TFSMParam.p, "32");
    DEFAULTS.put(TFSMParam.c, getClassAndJarFile());
    DEFAULTS.put(TFSMParam.m, "0.8");
    DEFAULTS.put(TFSMParam.d, "true");
    DEFAULTS.put(TFSMParam.r, "true");
    DEFAULTS.put(TFSMParam.n, CanonicalLabel.MIN_DFS.toString());
    DEFAULTS.put(TFSMParam.f, FilterStrategy.BROADCAST_JOIN.toString());
    DEFAULTS.put(TFSMParam.g, GrowthStrategy.FUSION.toString());
    DEFAULTS.put(
      TFSMParam.t, IterationStrategy.BULK_ITERATION.toString());
    DEFAULTS.put(
      TFSMParam.i, HDFS_PATH + PREDICTABLE_PREFIX + "1000_1.tlf");
    DEFAULTS.put(TFSMParam.l, FOLDER + "l.csv");
  }

  private static String getClassAndJarFile() {
    return TFSMBenchmark.class.getName() + " gradoop-examples-0.2-SNAPSHOT.jar";
  }

  public static void main(String[] args) throws ParseException, IOException {
    CommandLine cmd =
      parseArguments(args, TFSMBenchmarkScriptGenerator.class.getName());

    if (cmd == null) {
      return;
    }

    String outPath;
    if (cmd.hasOption(BENCHMARK)) {
      addBenchmarkLines();
      outPath = "benchmark.sh";

    } else if (cmd.hasOption(DATA)) {
      outPath = "generate_data.sh";

    } else {
      throw new IllegalArgumentException("No option provided. Please choose " +
        "-b for bechmark script or -d for data generator script.");
    }

    File file = new File(FOLDER + outPath);
    FileUtils.writeStringToFile(file, StringUtils.join(ROWS, "\n"));
  }

  private static void addBenchmarkLines() {
    addGraphCountBenchmark();
    addPartitionsBenchmark();
    addThresholdBenchmark();
  }

  private static void addPartitionsBenchmark() {
    Map<TFSMParam, String> params = Maps.newHashMap(DEFAULTS);

    // predictable

    for (int count : new Integer[] {4, 8, 16}) {
      params.put(TFSMParam.p, String.valueOf(count));
      addRoundLines(params);
    }

    // yeast
    params.put(TFSMParam.d, String.valueOf(false));
    params.put(TFSMParam.m, String.valueOf(0.5f));

    for (int count : new Integer[] {4, 8, 16}) {
      params.put(TFSMParam.p, String.valueOf(count));
      addRoundLines(params);
    }
  }

  private static void addThresholdBenchmark() {
    Map<TFSMParam, String> params = Maps.newHashMap(DEFAULTS);

    // predictable

    for (float minSupport : new Float[] {1.0f, 0.6f, 0.4f, 0.2f}) {
      params.put(TFSMParam.m, String.valueOf(minSupport));
      addRoundLines(params);
    }

    // yeast
    params.put(TFSMParam.d, String.valueOf(false));

    for (float minSupport : new Float[] {0.6f, 0.3f, 0.2f, 0.1f}) {
      params.put(TFSMParam.m, String.valueOf(minSupport));
      addRoundLines(params);
    }
  }


  private static void addGraphCountBenchmark() {
    Map<TFSMParam, String> params = Maps.newHashMap(DEFAULTS);

    // predictable

    for (int count : new Integer[] {1000, 10000, 100000}) {
      params.put(TFSMParam.i,
        HDFS_PATH + PREDICTABLE_PREFIX + count + "_1.tlf");
      addRoundLines(params);
    }

    // yeast
    params.put(TFSMParam.d, String.valueOf(false));
    params.put(TFSMParam.m, String.valueOf(0.5f));

    for (int count : new Integer[] {1, 10, 100}) {
      params.put(TFSMParam.i,
        HDFS_PATH + YEAST_PREFIX + count + ".tlf");
      addRoundLines(params);
    }
  }

  private static void addRoundLines(Map<TFSMParam, String> params) {
    for (CanonicalLabel canonicalLabel : CanonicalLabel.values()) {
      for (GrowthStrategy growthStrategy : GrowthStrategy.values()) {
        for (IterationStrategy iterationStrategy : IterationStrategy.values()) {
          for (Boolean preProcessing : new Boolean[] {true, false}) {
            params.put(TFSMParam.n, canonicalLabel.toString());
            params.put(TFSMParam.g, growthStrategy.toString());
            params.put(TFSMParam.t, iterationStrategy.toString());
            params.put(TFSMParam.r, preProcessing.toString());

            List<String> parts = Lists.newArrayList();

            parts.add(FLINK);

            for (TFSMParam param : TFSMParam.values()) {
              parts.add("-" + param);
              parts.add(params.get(param));
            }

            String row = StringUtils.join(parts, " ");

            for (int round = 1; round <= ROUNDS; round++) {
              ROWS.add(row);
            }
          }
        }
      }
    }
  }
}
