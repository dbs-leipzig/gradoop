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
package org.gradoop.flink.algorithms.gelly.trianglecounting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.WritePropertyToGraphHeadMap;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

/**
 * Gradoop EPGM model wrapper for the Flink Gelly algorithm for triangle counting in a graph
 * {@link org.apache.flink.graph.library.TriangleEnumerator}.
 * Counts all triangles (closed triplets) in a graph, without taking the edge direction in account.
 * Returns the initial {@link BaseGraph} with the number of triangles written as property to the
 * {@code GraphHead}. The value is accessed via the property key in {@link #PROPERTY_KEY_TRIANGLES}.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class GellyTriangleCounting<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  extends GradoopGellyAlgorithm<G, V, E, LG, GC, NullValue, NullValue> {

  /**
   * Property key to access the value for counted triangles in the graph head
   */
  public static final String PROPERTY_KEY_TRIANGLES = "triangle_count";

  /**
   * Creates an instance of GellyTriangleCounting.
   * Calls constructor of super class {@link GradoopGellyAlgorithm}.
   */
  public GellyTriangleCounting() {
    super(new VertexToGellyVertexWithNullValue<>(),
      new EdgeToGellyEdgeWithNullValue<>());
  }

  @Override
  public LG executeInGelly(Graph<GradoopId, NullValue, NullValue> gellyGraph) throws Exception {
    DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> triangles =
      new org.apache.flink.graph.library.TriangleEnumerator<GradoopId, NullValue, NullValue>()
        .run(gellyGraph);

    DataSet<G> resultHead = currentGraph.getGraphHead()
      .map(new WritePropertyToGraphHeadMap<>(
        PROPERTY_KEY_TRIANGLES, PropertyValue.create(triangles.count())));

    return currentGraph.getFactory().fromDataSets(
      resultHead, currentGraph.getVertices(), currentGraph.getEdges());
  }
}
