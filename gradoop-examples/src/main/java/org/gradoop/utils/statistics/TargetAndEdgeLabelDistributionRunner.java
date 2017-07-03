
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsReader;
import org.gradoop.flink.model.impl.operators.statistics.TargetLabelAndEdgeLabelDistribution;

/**
 * Computes {@link TargetLabelAndEdgeLabelDistribution} for a given logical graph.
 */
public class TargetAndEdgeLabelDistributionRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    new TargetLabelAndEdgeLabelDistribution()
      .execute(readLogicalGraph(args[0], args[1]))
      .map(value -> Tuple3.of(value.f0.f0, value.f0.f1, value.f1))
      .returns(new TypeHint<Tuple3<String, String, Long>>() { })
      .writeAsCsv(
        appendSeparator(args[2]) +
          GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    getExecutionEnvironment().execute("Statistics: Target and edge label distribution");
  }

  @Override
  public String getDescription() {
    return TargetAndEdgeLabelDistributionRunner.class.getName();
  }
}
