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
package org.gradoop.flink.algorithms.gelly.connectedcomponents;

import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.PropertyRemover;

/**
 * Computes the weakly connected components of a graph. Uses the gradoop wrapper
 * {@link AnnotateWeaklyConnectedComponents} of Flinks ConnectedComponents.
 * Splits the resulting {@link LogicalGraph} into a {@link GraphCollection} of its weakly connected
 * components.
 */
public class WeaklyConnectedComponentsAsCollection implements UnaryGraphToCollectionOperator {

  /**
   * Default property key to temporarily store the component id.
   */
  private static final String DEFAULT_PROPERTY_KEY = "wcc_component_id";

  /**
   * Property key to temporarily store the component id.
   */
  private final String propertyKey;

  /**
   * Maximum number of iterations.
   */
  private final int maxIterations;

  /**
   * Initialize the operator.
   *
   * @param maxIterations Maximum number of iterations for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   */
  public WeaklyConnectedComponentsAsCollection(int maxIterations) {
    this(DEFAULT_PROPERTY_KEY, maxIterations);
  }

  /**
   * Initialize the operator.
   *
   * @param propertyKey   Property key to temporarily store the component id.
   * @param maxIterations Maximum number of iteration for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   */
  public WeaklyConnectedComponentsAsCollection(String propertyKey, int maxIterations) {
    this.propertyKey = propertyKey;
    this.maxIterations = maxIterations;
  }

  @Override
  public GraphCollection execute(LogicalGraph graph) {

    LogicalGraph graphWithWccIds = graph.callForGraph(new AnnotateWeaklyConnectedComponents(
      propertyKey, maxIterations));

    GraphCollection split = graphWithWccIds.splitBy(propertyKey);

    return graph.getConfig().getGraphCollectionFactory().fromDataSets(
      split.getGraphHeads(),
      split.getVertices().map(new PropertyRemover<>(propertyKey)),
      split.getEdges().map(new PropertyRemover<>(propertyKey)));
  }

  @Override
  public String getName() {
    return WeaklyConnectedComponentsAsCollection.class.getName();
  }
}
