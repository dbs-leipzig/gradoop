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
import org.gradoop.flink.model.impl.operators.sampling.statistics.ConnectedComponentsDistribution;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.statistics.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;

/**
 * Calls the computation of the weakly connected components for a logical graph. Uses the
 * Gradoop-Wrapper {@link ConnectedComponentsDistribution} of Flinks ConnectedComponents-algorithm
 * with a given or default maximum number of iterations. Writes the result to a csv-file named
 * {@value SamplingEvaluationConstants#FILE_CONNECTED_COMPONENTS_DIST} in the output directory,
 * containing a line for each connected component. A line contains the components id followed by
 * the number of vertices and edges (if edges should be annotated) associated with this component.
 * If edges are not annotated, the value for edges will be {@code -1}, e.g.:
 * <pre>
 * BOF
 * 00000000000000000000000a, 10, 20
 * 00000000000000000000000b, 5, 10
 * EOF
 * </pre>
 */
public class ConnectedComponentsDistributionRunner extends AbstractRunner implements ProgramDescription {

  /**
   * Default value for maximum number of iterations
   */
  private static final int DEFAULT_MAX_ITERATIONS = Integer.MAX_VALUE;

  /**
   * Default value whether to annotate and count edges
   */
  private static final boolean DEFAULT_ANNOTATE_EDGES = true;

  /**
   * Calls the computation of the weakly connected components for the graph.
   *
   * <pre>
   * args[0] - path to graph
   * args[1] - format of graph (csv, json, indexed)
   * args[2] - output path
   * args[3] - maximum number of iterations for the algorithm, e.g. 10
   * args[4] - whether to annotate and count the edges (true) or not (false), e.g. true
   * </pre>
   *
   * @param args command line arguments
   * @throws Exception in case of read/write failure
   */
  public static void main(String[] args) throws Exception {

    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    int maxIteration = DEFAULT_MAX_ITERATIONS;
    boolean annotateEdges = DEFAULT_ANNOTATE_EDGES;
    String propertyKey = SamplingEvaluationConstants.PROPERTY_KEY_WCC_ID;

    if (args.length == 5) {
      maxIteration = Integer.parseInt(args[3]);
      annotateEdges = Boolean.parseBoolean(args[4]);
    }

    StatisticWriter.writeCSV(new ConnectedComponentsDistribution(
      propertyKey, maxIteration, annotateEdges).execute(graph), appendSeparator(args[2]) +
        SamplingEvaluationConstants.FILE_CONNECTED_COMPONENTS_DIST);

    getExecutionEnvironment().execute("Sampling Statistics: Connected components distribution");
  }

  @Override
  public String getDescription() {
    return ConnectedComponentsDistributionRunner.class.getName();
  }
}
