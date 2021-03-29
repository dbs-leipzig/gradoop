/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.AnnotateWeaklyConnectedComponents;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.functions.AggregateListOfWccEdges;
import org.gradoop.flink.model.impl.operators.statistics.functions.AggregateListOfWccVertices;
import org.gradoop.flink.model.impl.operators.statistics.functions.GetConnectedComponentDistributionFlatMap;

/**
 * Computes the weakly connected components of a graph. Uses the gradoop wrapper
 * {@link AnnotateWeaklyConnectedComponents} of Flinks ConnectedComponents.
 * Returns a {@code Tuple3<String, Long, Long>}, containing the component id and the number of
 * graph elements (vertices and edges) associated with it.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class ConnectedComponentsDistribution<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToValueOperator<LG, DataSet<Tuple3<String, Long, Long>>> {

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
  public DataSet<Tuple3<String, Long, Long>> execute(LG graph) {

    LG graphWithWccIds = graph.callForGraph(new AnnotateWeaklyConnectedComponents<>(
      propertyKey, maxIterations, annotateEdges));

    graphWithWccIds = graphWithWccIds.aggregate(new AggregateListOfWccVertices(propertyKey));
    if (annotateEdges) {
      graphWithWccIds = graphWithWccIds.aggregate(new AggregateListOfWccEdges(propertyKey));
    }

    return graphWithWccIds.getGraphHead().flatMap(
      new GetConnectedComponentDistributionFlatMap<>(propertyKey, annotateEdges));
  }
}
