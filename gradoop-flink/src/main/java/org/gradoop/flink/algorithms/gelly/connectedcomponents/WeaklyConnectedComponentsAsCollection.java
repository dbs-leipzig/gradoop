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
package org.gradoop.flink.algorithms.gelly.connectedcomponents;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphCollectionOperator;

/**
 * Computes the weakly connected components of a graph. Uses the gradoop wrapper
 * {@link AnnotateWeaklyConnectedComponents} of Flinks ConnectedComponents.
 * Splits the resulting {@link BaseGraph} into a {@link BaseGraphCollection} of its weakly connected
 * components.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class WeaklyConnectedComponentsAsCollection<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToBaseGraphCollectionOperator<LG, GC> {

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
  public GC execute(LG graph) {

    LG graphWithWccIds = graph.callForGraph(
      new AnnotateWeaklyConnectedComponents<>(propertyKey, maxIterations));

    return graphWithWccIds.splitBy(propertyKey);
  }
}
