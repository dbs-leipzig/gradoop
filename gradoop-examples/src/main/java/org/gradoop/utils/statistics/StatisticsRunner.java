
package org.gradoop.utils.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;

/**
 * Computes all statistics for a given logical graph.
 */
public class StatisticsRunner extends AbstractRunner implements ProgramDescription {

  /**
   * args[0] - path to input directory
   * args[1] - input format (json, csv)
   * args[2] - path to output directory
   *
   * @param args arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    VertexCountRunner.main(args);
    EdgeCountRunner.main(args);
    VertexLabelDistributionRunner.main(args);
    EdgeLabelDistributionRunner.main(args);
    VertexDegreeDistributionRunner.main(args);
    VertexOutgoingDegreeDistributionRunner.main(args);
    VertexIncomingDegreeDistributionRunner.main(args);
    DistinctSourceVertexCountRunner.main(args);
    DistinctTargetVertexCountRunner.main(args);
    DistinctSourceVertexCountByEdgeLabelRunner.main(args);
    DistinctTargetVertexCountByEdgeLabelRunner.main(args);
    SourceAndEdgeLabelDistributionRunner.main(args);
    TargetAndEdgeLabelDistributionRunner.main(args);
    DistinctEdgePropertiesByLabelRunner.main(args);
    DistinctVertexPropertiesByLabelRunner.main(args);
    DistinctEdgePropertiesRunner.main(args);
    DistinctVertexPropertiesRunner.main(args);
  }

  @Override
  public String getDescription() {
    return "Graph Statistics";
  }
}
