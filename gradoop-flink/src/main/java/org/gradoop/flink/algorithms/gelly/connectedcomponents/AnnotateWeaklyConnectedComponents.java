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
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.connectedcomponents.functions.ConnectedComponentToAttribute;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithGradoopId;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.ConnectedComponents}.
 * The result will be the same {@link LogicalGraph} with a component id assigned to each vertex
 * as a Property.
 */
public class AnnotateWeaklyConnectedComponents extends GellyAlgorithm<GradoopId, NullValue> {

  /**
   * Property key to store the component id in.
   */
  private final String propertyKey;

  /**
   * Maximum number of iterations.
   */
  private final int maxIterations;

  /**
   * Constructor for connected components with a maximum number of iterations.
   *
   * @param propertyKey   Property key to store the component id in.
   * @param maxIterations The maximum number of iterations.
   */
  public AnnotateWeaklyConnectedComponents(String propertyKey, int maxIterations) {
    super(new VertexToGellyVertexWithGradoopId(), new EdgeToGellyEdgeWithNullValue());
    this.propertyKey = propertyKey;
    this.maxIterations = maxIterations;
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, GradoopId, NullValue> graph)
    throws Exception {
    DataSet<Vertex> newVertices = new org.apache.flink.graph.library.ConnectedComponents<GradoopId,
      GradoopId, NullValue>(maxIterations)
      .run(graph)
      .join(currentGraph.getVertices())
      .where(0)
      .equalTo(new Id<>())
      .with(new ConnectedComponentToAttribute(propertyKey));
    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices,
      currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return getClass().getName();
  }
}
