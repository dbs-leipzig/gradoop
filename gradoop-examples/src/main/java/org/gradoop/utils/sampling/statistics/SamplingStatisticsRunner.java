/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.utils.sampling.statistics;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.utils.statistics.VertexDegreeDistributionRunner;
import org.gradoop.utils.statistics.VertexIncomingDegreeDistributionRunner;
import org.gradoop.utils.statistics.VertexOutgoingDegreeDistributionRunner;

/**
 * Calls the computation of all given graph properties for a logical graph. Results are written
 * to files in the output path.
 */
public class SamplingStatisticsRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Calls the computation of all given graph properties.
   * (List of called graph properties will be extended over time)
   *
   * <pre>
   * args[0] - path to graph
   * args[1] - format of graph (csv, json, indexed)
   * args[2] - output path
   * </pre>
   *
   * @param args command line arguments
   * @throws Exception if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    GraphDensityRunner.main(args);
    VertexDegreeDistributionRunner.main(args);
    VertexOutgoingDegreeDistributionRunner.main(args);
    VertexIncomingDegreeDistributionRunner.main(args);
    AverageDegreeRunner.main(args);
    AverageIncomingDegreeRunner.main(args);
    AverageOutgoingDegreeRunner.main(args);
    ConnectedComponentsDistributionRunner.main(args);
  }

  @Override
  public String getDescription() {
    return "Sampling Statistics";
  }
}
