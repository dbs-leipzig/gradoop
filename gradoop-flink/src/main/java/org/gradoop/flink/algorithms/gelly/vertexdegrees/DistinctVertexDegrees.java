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
package org.gradoop.flink.algorithms.gelly.vertexdegrees;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.functions.DistinctVertexDegreesToAttribute;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees}.
 * Multiple edges between two vertices will only be counted once per direction.
 * <p>
 * Note: This Gelly implementation count loops between edges like (v1) -> (v2),
 * (v2) -> (v1) as one.
 */
public class DistinctVertexDegrees extends GellyAlgorithm<NullValue, NullValue> {

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
   * Constructor for Vertex Degree with in- and out-degree and total of degrees of a graph.
   *
   * @param propertyKey Property key to store the sum of in- and out-degrees in.
   * @param propertyKeyIn Property key to store the in-degree in.
   * @param propertyKeyOut Property key to store the out-degree in.
   */
  public DistinctVertexDegrees(String propertyKey, String propertyKeyIn, String propertyKeyOut) {
    this(propertyKey, propertyKeyIn, propertyKeyOut, true);
  }

  /**
   * Constructor for Vertex Degree with fixed set of whether to output
   * vertices with an in-degree of zero.
   *
   * @param propertyKey Property key to store the sum of in- and out-degrees in.
   * @param propertyKeyIn Property key to store the in-degree in.
   * @param propertyKeyOut Property key to store the out-degree in.
   * @param includeZeroDegreeVertices whether to output vertices with an
   *                                  in-degree of zero
   */
  public DistinctVertexDegrees(String propertyKey, String propertyKeyIn, String propertyKeyOut,
    boolean includeZeroDegreeVertices) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithNullValue());
    this.propertyKey = propertyKey;
    this.propertyKeyIn = propertyKeyIn;
    this.propertyKeyOut = propertyKeyOut;
    this.includeZeroDegreeVertices = includeZeroDegreeVertices;
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph)
    throws Exception {
    DataSet<Vertex> newVertices =
      new org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees<GradoopId, NullValue,
      NullValue>()
      .setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
      .run(graph)
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new DistinctVertexDegreesToAttribute(propertyKey, propertyKeyIn, propertyKeyOut));

    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices,
      currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return DistinctVertexDegrees.class.getName();
  }
}
