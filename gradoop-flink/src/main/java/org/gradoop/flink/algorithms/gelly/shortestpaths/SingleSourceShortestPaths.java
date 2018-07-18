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
package org.gradoop.flink.algorithms.gelly.shortestpaths;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithDouble;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.shortestpaths.functions.SingleSourceShortestPathsAttribute;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.SingleSourceShortestPaths}.
 *
 */
public class SingleSourceShortestPaths extends GellyAlgorithm<NullValue, Double> {

  /**
   * ID of the source vertex
   */
  private final GradoopId srcVertexId;
  /**
   * Number of iterations.
   */
  private final int iterations;

  /**
   * Property key to store the vertex id in.
   */
  private final String propertyKeyVertex;

  /**
   * Property key to store the edge id in.
   */
  private final String propertyKeyEdge;

  /**
   * Constructor for single source shortest paths with the {@link GradoopId} of the source vertex
   * and a maximum number of iterations.
   *
   * @param srcVertexId Id of the source vertex.
   * @param propertyKeyEdge Property key to store the edge value in.
   * @param iterations The maximum number of iterations.
   * @param propertyKeyVertex Property key to store the vertex value in.
   */
  public SingleSourceShortestPaths(GradoopId srcVertexId, String propertyKeyEdge,
    int iterations, String propertyKeyVertex) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithDouble(propertyKeyEdge));
    this.propertyKeyVertex = propertyKeyVertex;
    this.propertyKeyEdge = propertyKeyEdge;
    this.iterations = iterations;
    this.srcVertexId = srcVertexId;
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, Double> graph)
    throws Exception {

    DataSet<Vertex> newVertices = new org.apache.flink.graph.library.SingleSourceShortestPaths
      <GradoopId, NullValue>(srcVertexId, iterations)
      .run(graph)
      .join(currentGraph.getVertices())
      .where(0)
      .equalTo(new Id<>())
      .with(new SingleSourceShortestPathsAttribute(propertyKeyVertex));
    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices,
      currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return SingleSourceShortestPaths.class.getName();
  }
}
