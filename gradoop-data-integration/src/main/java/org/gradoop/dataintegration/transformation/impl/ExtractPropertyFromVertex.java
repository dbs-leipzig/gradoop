/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.transformation.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.transformation.impl.config.EdgeDirection;
import org.gradoop.dataintegration.transformation.impl.functions.CreateNewEdges;
import org.gradoop.dataintegration.transformation.impl.functions.CreateNewVertex;
import org.gradoop.dataintegration.transformation.impl.functions.CreateNewVertexWithEqualityCondense;
import org.gradoop.dataintegration.transformation.impl.functions.ExtractPropertyWithOriginId;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;

import java.util.List;

/**
 * This operator is executed on the vertex dataset and extracts a property to a newly created vertex
 * which can be connected to the vertex it was created from. If the property is a list every
 * element of the list is a new vertex.
 */
public class ExtractPropertyFromVertex implements UnaryGraphToGraphOperator {

  /**
   * The vertices the extraction is executed for.
   */
  private String forVerticesOfLabel;

  /**
   * The property key at the original vertex.
   */
  private String originalPropertyName;

  /**
   * The label of the extracted vertex.
   */
  private String newVertexLabel;

  /**
   * The property name of the extracted property at the new vertex.
   */
  private String newPropertyName;

  /**
   * The direction of the created edge(s).
   */
  private EdgeDirection edgeDirection;

  /**
   * The label of the newly created edge(s).
   */
  private String edgeLabel;

  /**
   * If true, all property values got condensed based on their equality.
   * So that every newly created vertex has a unique property value.
   * The default value is 'true'.
   */
  private boolean condense = true;

  /**
   * Constructs a new {@link UnaryGraphToGraphOperator} which extracts properties from vertices into
   * newly created vertices.
   *
   * @param forVerticesOfLabel   The vertices the extraction is executed for.
   * @param originalPropertyName The property key at the original vertex.
   * @param newVertexLabel       The label of the extracted vertex.
   * @param newPropertyName      The property name of the extracted property at the new vertex.
   */
  public ExtractPropertyFromVertex(String forVerticesOfLabel, String originalPropertyName,
                                   String newVertexLabel, String newPropertyName) {
    this(forVerticesOfLabel, originalPropertyName, newVertexLabel, newPropertyName,
      EdgeDirection.NONE, null);
  }

  /**
   * Constructs a new {@link UnaryGraphToGraphOperator} which extracts properties from vertices into
   * newly created vertices and connects original and new vertex with each other.
   *
   * @param forVerticesOfLabel   The vertices the extraction is executed for.
   * @param originalPropertyName The property key at the original vertex.
   * @param newVertexLabel       The label of the extracted vertex.
   * @param newPropertyName      The property name of the extracted property at the new vertex.
   * @param edgeDirection        The direction of the created edge(s).
   * @param edgeLabel            The label of the newly created edge.
   */
  public ExtractPropertyFromVertex(String forVerticesOfLabel, String originalPropertyName,
                                   String newVertexLabel, String newPropertyName,
                                   EdgeDirection edgeDirection, String edgeLabel) {
    this.forVerticesOfLabel = Preconditions.checkNotNull(forVerticesOfLabel);
    this.originalPropertyName = Preconditions.checkNotNull(originalPropertyName);
    this.newVertexLabel = Preconditions.checkNotNull(newVertexLabel);
    this.newPropertyName = Preconditions.checkNotNull(newPropertyName);
    this.edgeDirection = Preconditions.checkNotNull(edgeDirection);
    if (!edgeDirection.equals(EdgeDirection.NONE)) {
      this.edgeLabel = Preconditions.checkNotNull(edgeLabel);
    }
  }

  /**
   * This method sets whether property value based condensation should be executed or not.
   * The default value is 'true'.
   *
   * @param condense If true, all property values got condensed based on their equality.
   *                 So that every newly created vertex has a unique property value.
   */
  public void setCondensation(boolean condense) {
    this.condense = condense;
  }

  @Override
  public LogicalGraph execute(LogicalGraph logicalGraph) {
    // filter the vertices by the given label
    DataSet<Vertex> filteredVertices = logicalGraph
      .getVertices()
      .filter(new ByLabel<>(forVerticesOfLabel));

    // calculate new vertices and store the origin for linking
    DataSet<Tuple2<PropertyValue, GradoopId>> candidates = filteredVertices
      .flatMap(new ExtractPropertyWithOriginId(originalPropertyName));

    // extract the new vertices
    DataSet<Tuple2<Vertex, List<GradoopId>>> newVerticesAndOriginIds;
    if (condense) {
      newVerticesAndOriginIds = candidates
        .groupBy(0)
        .reduceGroup(new CreateNewVertexWithEqualityCondense(
          logicalGraph.getConfig().getVertexFactory(), newVertexLabel, newPropertyName));
    } else {
      newVerticesAndOriginIds = candidates
        .map(new CreateNewVertex(logicalGraph.getConfig().getVertexFactory(), newVertexLabel,
          newPropertyName));
    }

    DataSet<Vertex> vertices = newVerticesAndOriginIds
      .map(new Value0Of2<>())
      .union(logicalGraph.getVertices());

    // the newly created vertices should be linked to the original vertices
    DataSet<Edge> edges = logicalGraph.getEdges();
    if (!edgeDirection.equals(EdgeDirection.NONE)) {
      edges = newVerticesAndOriginIds
        .flatMap(new CreateNewEdges(logicalGraph.getConfig().getEdgeFactory(), edgeDirection,
          edgeLabel))
        .union(edges);
    }

    return logicalGraph.getConfig()
      .getLogicalGraphFactory()
      .fromDataSets(logicalGraph.getGraphHead(), vertices, edges);
  }
}
