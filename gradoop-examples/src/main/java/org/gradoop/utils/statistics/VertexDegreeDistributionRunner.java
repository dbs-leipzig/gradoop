
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegreeDistribution;

/**
 * Computes {@link VertexDegreeDistribution} for a given logical graph.
 */
public class VertexDegreeDistributionRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    new VertexDegreeDistribution()
      .execute(readLogicalGraph(args[0], args[1]))
      .writeAsCsv(appendSeparator(args[2]) + "vertex_degree_distribution")
      .setParallelism(1);

    getExecutionEnvironment().execute("Statistics: Vertex degree distribution");
  }

  @Override
  public String getDescription() {
    return VertexDegreeDistributionRunner.class.getName();
  }
}
