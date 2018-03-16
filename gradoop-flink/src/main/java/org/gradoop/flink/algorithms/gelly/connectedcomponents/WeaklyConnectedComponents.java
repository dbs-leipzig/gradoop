/**
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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.PropertyRemover;

/**
 * Split a {@link LogicalGraph} into a {@link GraphCollection} of its weakly connected components.
 */
public class WeaklyConnectedComponents implements UnaryGraphToCollectionOperator {

  /**
   * Default property key to temporarily store the component id.
   */
  private static final String DEFAULT_PROPERTY_KEY = "_wcc_component_id";

  /**
   * Maximum number of iterations for;
   */
  private final int maxIterations;

  /**
   * Property key to temporarily store the component id.
   */
  private final String propertyKey;

  /**
   * Initialize the operator using the default property key.
   *
   * @param maxIterations Maximum number of iterations for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   */
  public WeaklyConnectedComponents(int maxIterations) {
    this(DEFAULT_PROPERTY_KEY, maxIterations);
  }

  /**
   * Initialize the operator.
   *
   * @param propertyKey   Property key to temporarily store the component id.
   * @param maxIterations Maximum number of iteration for
   *                      {@link AnnotateWeaklyConnectedComponents}.
   */
  public WeaklyConnectedComponents(String propertyKey, int maxIterations) {
    this.maxIterations = maxIterations;
    this.propertyKey = propertyKey;
  }


  @Override
  public GraphCollection execute(LogicalGraph graph) {
    LogicalGraph withWccAnnotations = graph
      .callForGraph(new AnnotateWeaklyConnectedComponents(propertyKey, maxIterations));
    GraphCollection split = withWccAnnotations.splitBy(propertyKey);
    DataSet<Vertex> vertices = split.getVertices()
      .map(new PropertyRemover<>(propertyKey));
    return graph.getConfig().getGraphCollectionFactory().fromDataSets(split.getGraphHeads(),
      vertices, split.getEdges());
  }

  @Override
  public String getName() {
    return WeaklyConnectedComponents.class.getName();
  }
}
