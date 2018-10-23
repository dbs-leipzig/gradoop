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
package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.AnnotateWeaklyConnectedComponents;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.sampling.statistics.functions.AggregateListOfWccEdges;
import org.gradoop.flink.model.impl.operators.sampling.statistics.functions.AggregateListOfWccVertices;
import org.gradoop.flink.model.impl.operators.sampling.statistics.functions.GetConnectedComponentDistributionFlatMap;

/**
 * Computes the weakly connected components of a graph. Uses the gradoop wrapper
 * {@link AnnotateWeaklyConnectedComponents} of Flinks ConnectedComponents.
 * Writes a mapping to the graph head, containing the component id and the number of graph elements
 * (vertices and edges) associated with it.
 */
public class ConnectedComponentsDistribution implements UnaryGraphToValueOperator {

  /**
   * Property key to store the component id.
   */
  private final String propertyKey;

  /**
   * Maximum number of iterations.
   */
  private final int maxIterations;

  /**
   * Whether to write the component property to the edges
   */
  private final boolean annotateEdges;

  /**
   * Constructor
   *
   * @param maxIterations Maximum number of iterations for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   */
  public ConnectedComponentsDistribution(int maxIterations) {
    this(SamplingEvaluationConstants.PROPERTY_KEY_WCC_ID, maxIterations, false);
  }

  /**
   * Constructor
   *
   * @param maxIterations Maximum number of iterations for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   * @param annotateEdges Whether to write the component property to the edges
   */
  public ConnectedComponentsDistribution(int maxIterations, boolean annotateEdges) {
    this(SamplingEvaluationConstants.PROPERTY_KEY_WCC_ID, maxIterations, annotateEdges);
  }

  /**
   * Constructor
   *
   * @param propertyKey Property key to temporarily store the component id.
   * @param maxIterations Maximum number of iterations for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   */
  public ConnectedComponentsDistribution(String propertyKey, int maxIterations) {
    this(propertyKey, maxIterations, false);
  }

  /**
   * Constructor
   *
   * @param propertyKey Property key to temporarily store the component id.
   * @param maxIterations Maximum number of iterations for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   * @param annotateEdges Whether to write the component property to the edges
   */
  public ConnectedComponentsDistribution(String propertyKey, int maxIterations,
    boolean annotateEdges) {
    this.propertyKey = propertyKey;
    this.maxIterations = maxIterations;
    this.annotateEdges = annotateEdges;
  }

  @Override
  public DataSet<Tuple3<String, Long, Long>> execute(LogicalGraph graph) {

    LogicalGraph graphWithWccIds = graph.callForGraph(new AnnotateWeaklyConnectedComponents(
      propertyKey, maxIterations, annotateEdges));

    graphWithWccIds = graphWithWccIds.aggregate(new AggregateListOfWccVertices(propertyKey));
    if (annotateEdges) {
      graphWithWccIds = graphWithWccIds.aggregate(new AggregateListOfWccEdges(propertyKey));
    }

    return graphWithWccIds.getGraphHead().flatMap(
      new GetConnectedComponentDistributionFlatMap(propertyKey, annotateEdges));
  }
}
