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
package org.gradoop.flink.algorithms.gelly.vertexdegrees;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.functions.DistinctVertexDegreesToAttribute;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees}.
 * Multiple edges between two vertices will only be counted once per direction.
 * <p>
 * Note: This Gelly implementation count loops between edges like {@code (v1) -> (v2)},
 * {@code (v2) -> (v1)} as one.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class DistinctVertexDegrees<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  extends GradoopGellyAlgorithm<G, V, E, LG, GC, NullValue, NullValue> {

  /**
   * Property key to store the sum vertex degree in.
   */
  private final String propertyKey;
  /**
   * Property key to store the in vertex degree in.
   */
  private final String propertyKeyIn;
  /**
   * Property key to store the out vertex degree in.
   */
  private final String propertyKeyOut;

  /**
   * Output vertices with an in-degree of zero.
   */
  private final boolean includeZeroDegreeVertices;

  /**
   * Constructor for vertex degree with in- and out-degree and total of degrees of a graph.
   *
   * @param propertyKey Property key to store the sum of in- and out-degrees in.
   * @param propertyKeyIn Property key to store the in-degree in.
   * @param propertyKeyOut Property key to store the out-degree in.
   */
  public DistinctVertexDegrees(String propertyKey, String propertyKeyIn, String propertyKeyOut) {
    this(propertyKey, propertyKeyIn, propertyKeyOut, true);
  }

  /**
   * Constructor for vertex degree with fixed set of whether to output vertices with an in-degree of zero.
   *
   * @param propertyKey Property key to store the sum of in- and out-degrees in.
   * @param propertyKeyIn Property key to store the in-degree in.
   * @param propertyKeyOut Property key to store the out-degree in.
   * @param includeZeroDegreeVertices whether to output vertices with an
   *                                  in-degree of zero
   */
  public DistinctVertexDegrees(String propertyKey, String propertyKeyIn, String propertyKeyOut,
    boolean includeZeroDegreeVertices) {
    super(new VertexToGellyVertexWithNullValue<>(), new EdgeToGellyEdgeWithNullValue<>());
    this.propertyKey = propertyKey;
    this.propertyKeyIn = propertyKeyIn;
    this.propertyKeyOut = propertyKeyOut;
    this.includeZeroDegreeVertices = includeZeroDegreeVertices;
  }

  @Override
  public LG executeInGelly(Graph<GradoopId, NullValue, NullValue> gellyGraph) throws Exception {
    DataSet<V> newVertices =
      new org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees<GradoopId, NullValue, NullValue>()
      .setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
      .run(gellyGraph)
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new DistinctVertexDegreesToAttribute<>(propertyKey, propertyKeyIn, propertyKeyOut));

    return currentGraph.getFactory()
      .fromDataSets(currentGraph.getGraphHead(), newVertices, currentGraph.getEdges());
  }
}
