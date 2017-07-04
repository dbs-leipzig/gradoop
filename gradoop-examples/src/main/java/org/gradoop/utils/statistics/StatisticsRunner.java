/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
