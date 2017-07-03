
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsReader;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIdsByEdgeLabel;

/**
 * Computes {@link DistinctTargetIdsByEdgeLabel} for a given logical graph.
 */
public class DistinctTargetVertexCountByEdgeLabelRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    new DistinctTargetIdsByEdgeLabel()
      .execute(readLogicalGraph(args[0], args[1]))
      .writeAsCsv(
        appendSeparator(args[2]) +
          GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL,
        System.lineSeparator(), GraphStatisticsReader.TOKEN_SEPARATOR)
      .setParallelism(1);

    getExecutionEnvironment().execute("Statistics: Distinct target vertex count by edge label");
  }

  @Override
  public String getDescription() {
    return DistinctTargetVertexCountByEdgeLabelRunner.class.getName();
  }
}
