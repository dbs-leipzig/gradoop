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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.transformation.functions.AccumulatePropagatedValues;
import org.gradoop.dataintegration.transformation.functions.BuildIdPropertyValuePairs;
import org.gradoop.dataintegration.transformation.functions.BuildTargetVertexIdPropertyValuePairs;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.LabelIsIn;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraphBroadcast;

import java.util.Objects;
import java.util.Set;

/**
 * A property of a vertex is propagated to its neighbors and aggregated in a Property List.
 */
public class PropagatePropertyToNeighbor implements UnaryGraphToGraphOperator {

  /**
   * The label of the vertex the property to propagate is part of.
   */
  private final String vertexLabel;

  /**
   * The property key of the property to propagate.
   */
  private final String propertyKey;

  /**
   * The property key where the PropertyValue list should be stored at the target vertices.
   */
  private final String targetVertexPropertyKey;

  /**
   * Only edges with the inserted labels are used. If all labels are sufficient use {@code null}.
   */
  private final Set<String> propagatingEdgeLabels;

  /**
   * Only vertices with the inserted labels will store the propagated values.
   * If all vertices should do it use {@code null}.
   */
  private final Set<String> targetVertexLabels;

  /**
   * The constructor for the propagate property transformation. Additionally it is possible to
   * define which edge labels can be used for propagation and / or which vertices could be target
   * of the Properties.
   * <p>
   * Using this constructor, properties will be propagated along all edges and to all
   * target vertices. {@link #PropagatePropertyToNeighbor(String, String, String, Set, Set)}
   * can be used when properties should only be propagated along certain edges (selected by their
   * label) and / or to certain vertices (selected by their label). Using this constructor is
   * equivalent to {@code PropagatePropertyToNeighbor(vertexLabel, propertyKey,
   * targetVertexPropertyKey, null, null)}.
   *
   * @param vertexLabel             The label of the vertex the property to propagate is part of.
   * @param propertyKey             The property key of the property to propagate.
   * @param targetVertexPropertyKey The property key where the PropertyValue list should be stored
   *                                at the target vertices.
   */
  public PropagatePropertyToNeighbor(String vertexLabel, String propertyKey,
    String targetVertexPropertyKey) {
    this(vertexLabel, propertyKey, targetVertexPropertyKey, null, null);
  }

  /**
   * The constructor for the propagate property transformation. Additionally it is possible to
   * define which edge labels can be used for propagation and / or which vertices could be target
   * of the Properties.
   *
   * @param vertexLabel             The label of the vertex the property to propagate is part of.
   * @param propertyKey             The property key of the property to propagate.
   * @param targetVertexPropertyKey The property key where the PropertyValue list should be stored
   *                                at the target vertices.
   * @param propagatingEdges        Only edges with the inserted labels are used. If all labels
   *                                are sufficient use {@code null}.
   * @param targetVertexLabels      Only vertices with the inserted labels will store the
   *                                propagated values. If all vertices should, use {@code null}.
   */
  public PropagatePropertyToNeighbor(String vertexLabel, String propertyKey,
                                     String targetVertexPropertyKey, Set<String> propagatingEdges,
                                     Set<String> targetVertexLabels) {
    this.vertexLabel = Objects.requireNonNull(vertexLabel);
    this.propertyKey = Objects.requireNonNull(propertyKey);
    this.targetVertexPropertyKey = Objects.requireNonNull(targetVertexPropertyKey);
    this.propagatingEdgeLabels = propagatingEdges;
    this.targetVertexLabels = targetVertexLabels;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    // prepare the edge set, EdgeFilter if propagating edges are given
    DataSet<Edge> propagateAlong = graph.getEdges();
    if (propagatingEdgeLabels != null) {
      propagateAlong = propagateAlong.filter(new LabelIsIn<>(propagatingEdgeLabels));
    }

    DataSet<Vertex> newVertices = graph.getVertices()
      // Extract properties to propagate
      .flatMap(new BuildIdPropertyValuePairs<>(vertexLabel, propertyKey))
      // Propagate along edges.
      .join(propagateAlong)
      .where(0).equalTo(new SourceId<>())
      .with(new BuildTargetVertexIdPropertyValuePairs<>())
      // Update target vertices.
      .coGroup(graph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new AccumulatePropagatedValues<>(targetVertexPropertyKey, targetVertexLabels))
      .map(new AddToGraphBroadcast<>())
      .withBroadcastSet(graph.getGraphHead().map(new Id<>()), AddToGraphBroadcast.GRAPH_ID);

    return graph.getFactory().fromDataSets(graph.getGraphHead(), newVertices, graph.getEdges());
  }

  @Override
  public String getName() {
    return PropagatePropertyToNeighbor.class.getName();
  }
}
