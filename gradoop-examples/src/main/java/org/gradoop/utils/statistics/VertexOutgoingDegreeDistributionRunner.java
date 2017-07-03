
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.impl.operators.statistics.OutgoingVertexDegreeDistribution;

/**
 * Computes {@link OutgoingVertexDegreeDistribution} for a given logical graph.
 */
public class VertexOutgoingDegreeDistributionRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    new OutgoingVertexDegreeDistribution()
      .execute(readLogicalGraph(args[0], args[1]))
      .writeAsCsv(appendSeparator(args[2]) + "outgoing_vertex_degree_distribution")
      .setParallelism(1);

    getExecutionEnvironment().execute("Statistics: Vertex outgoing degree distribution");
  }

  @Override
  public String getDescription() {
    return VertexOutgoingDegreeDistributionRunner.class.getName();
  }
}
