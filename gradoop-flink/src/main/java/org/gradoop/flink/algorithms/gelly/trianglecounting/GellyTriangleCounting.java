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
package org.gradoop.flink.algorithms.gelly.trianglecounting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.WritePropertyToGraphHeadMap;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Gradoop EPGM model wrapper for the Flink Gelly algorithm for triangle counting in a graph
 * {@link org.apache.flink.graph.library.TriangleEnumerator}.
 * Counts all triangles (closed triplets) in a graph, without taking the edge direction in account.
 * Returns the initial {@code LogicalGraph} with the number of triangles written as property to the
 * {@code GraphHead}. The value is accessed via the property key in {@link #PROPERTY_KEY_TRIANGLES}.
 */
public class GellyTriangleCounting extends GellyAlgorithm<NullValue, NullValue> {

  /**
   * Property key to access the value for counted triangles in the graph head
   */
  public static final String PROPERTY_KEY_TRIANGLES = "triangle_count";

  /**
   * Creates an instance of GellyTriangleCounting.
   * Calls constructor of super class {@link GellyAlgorithm}.
   */
  public GellyTriangleCounting() {
    super(new VertexToGellyVertexWithNullValue(),
      new EdgeToGellyEdgeWithNullValue());
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph)
    throws Exception {
    DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> triangles =
      new org.apache.flink.graph.library.TriangleEnumerator<GradoopId, NullValue, NullValue>()
      .run(graph);

    DataSet<GraphHead> resultHead = currentGraph.getGraphHead()
      .map(new WritePropertyToGraphHeadMap(
        PROPERTY_KEY_TRIANGLES, PropertyValue.create(triangles.count())));

    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(
      resultHead, currentGraph.getVertices(), currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return GellyTriangleCounting.class.getName();
  }
}
