package org.gradoop.benchmark.fsm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.flink.algorithms.fsm.common.config.CanonicalLabel;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.GrowthStrategy;
import org.gradoop.flink.algorithms.fsm.common.config.IterationStrategy;

import java.util.List;
import java.util.Map;

/**
 * Created by peet on 29.09.16.
 */
public class TFSMBenchmarkScriptGenerator {

  //-p ${PARALLELISM} -c ${CLASS} ${JAR_FILE} -i${INPUT} ${ARGS}

  private static final String FLINK = "/usr/local/flink-1.0.3/bin/flink run";

  private static int ROUNDS = 3;

  public static final Map<TFSMParam, String> DEFAULTS = Maps.newHashMap();

  static {
    DEFAULTS.put(TFSMParam.p, "32");
    DEFAULTS.put(TFSMParam.c, getClassAndJarFile());
    DEFAULTS.put(TFSMParam.minSup, "0.8");
    DEFAULTS.put(TFSMParam.directed, "true");
    DEFAULTS.put(TFSMParam.pre, "true");
    DEFAULTS.put(TFSMParam.canlab, CanonicalLabel.MIN_DFS.toString());
    DEFAULTS.put(TFSMParam.f, FilterStrategy.BROADCAST_JOIN.toString());
    DEFAULTS.put(TFSMParam.g, GrowthStrategy.FUSION.toString());
    DEFAULTS.put(
      TFSMParam.i, IterationStrategy.BULK_ITERATION.toString());
  }

  private static String getClassAndJarFile() {
    return TFSMBenchmark.class.getName() + " gradoop-examples-0.2-SNAPSHOT.jar";
  }

  public static void main(String[] args) {
    for (int round = 1; round <= ROUNDS; round++) {
      List<String> parts = Lists.newArrayList();

      parts.add(FLINK);

      Map<TFSMParam, String> params = Maps.newHashMap(DEFAULTS);

      for (TFSMParam param : TFSMParam.values()) {
        parts.add("-" + param);
        parts.add(DEFAULTS.get(param));
      }

      System.out.println(StringUtils.join(parts, " "));
    }
  }
}
