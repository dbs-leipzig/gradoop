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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.transformation.functions.SetBasedLabelFilter;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A property of a vertex is propagated to its neighbors and aggregated in a Property List.
 */
public class PropagatePropertyToNeighbor implements UnaryGraphToGraphOperator {
  private final String vertexLabel;
  private final String propertyKey;
  private final String targetVertexPropertyKey;
  private final Set<String> propagatingEdges;
  private final Set<String> targetVertexLabels;

  /**
   * The constructor for the propagate property transformation. Additionally it is possible to
   * define which edge labels can be used for propagation and / or which vertices could be target
   * of the Properties.
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
   * @param propagatingEdges        only edges with the inserted labels are used. If all labels
   *                                are sufficient use 'null'
   * @param targetVertexLabels      only vertices with the inserted labels will store the propagated
   *                                values. If all vertices should do it use 'null'
   */
  public PropagatePropertyToNeighbor(String vertexLabel, String propertyKey,
                                     String targetVertexPropertyKey, Set<String> propagatingEdges,
                                     Set<String> targetVertexLabels) {
    this.vertexLabel = vertexLabel;
    this.propertyKey = propertyKey;
    this.targetVertexPropertyKey = targetVertexPropertyKey;
    this.propagatingEdges = propagatingEdges;
    this.targetVertexLabels = targetVertexLabels;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    // prepare the edge set, EdgeFilter if propagating edges are given
    DataSet<Edge> filteredEdges = graph.getEdges();
    if (propagatingEdges != null) {
      filteredEdges = filteredEdges.filter(new SetBasedLabelFilter<>(propagatingEdges));
    }

    // prepare the vertex set for target vertices
    DataSet<Vertex> filteredVertices = graph.getVertices();
    if (targetVertexLabels != null) {
      filteredVertices = filteredVertices.filter(new SetBasedLabelFilter<>(targetVertexLabels));
    }

    DataSet<Vertex> newVertices = graph
      .getVerticesByLabel(vertexLabel)
      .flatMap(new BuildIdPropertyValuePairs(propertyKey))
      .join(filteredEdges)
      .where(0).equalTo(new TargetId<>())
      .with(new BuildTargetVertexIdPropertyValueParis())
      .coGroup(filteredVertices)
      .where(0).equalTo(new Id<>())
      .with(new AccumulatePropagatedValues(targetVertexPropertyKey));

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(graph.getGraphHead(), graph.getVertices().union(newVertices),
        graph.getEdges());
  }

  @Override
  public String getName() {
    return PropagatePropertyToNeighbor.class.getName();
  }


  /**
   * A simple {@link FlatMapFunction} that prepares Vertex data for further processing.
   * Since not all vertices necessarily have the property a flat map is used.
   */
  private static class BuildIdPropertyValuePairs implements FlatMapFunction<Vertex,
    Tuple2<GradoopId, PropertyValue>> {

    /**
     * The property key of the property to propagate.
     */
    private final String propertyKey;

    /**
     * The constructor of the {@link FlatMapFunction} to create {@link GradoopId} /
     * {@link PropertyValue} pairs.
     *
     * @param propertyKey The property key of the property to propagate.
     */
    BuildIdPropertyValuePairs(String propertyKey) {
      this.propertyKey = propertyKey;
    }

    @Override
    public void flatMap(Vertex v, Collector<Tuple2<GradoopId, PropertyValue>> out) {
      PropertyValue pv = v.getPropertyValue(propertyKey);
      if (pv != null) {
        out.collect(Tuple2.of(v.getId(), pv));
      }
    }
  }

  /**
   * The {@link JoinFunction} builds new {@link GradoopId} / {@link PropertyValue} pairs.
   */
  private static class BuildTargetVertexIdPropertyValueParis
    implements JoinFunction<Tuple2<GradoopId, PropertyValue>, Edge,
    Tuple2<GradoopId, PropertyValue>> {

    @Override
    public Tuple2<GradoopId, PropertyValue> join(Tuple2<GradoopId, PropertyValue> t, Edge e) {
      return Tuple2.of(e.getSourceId(), t.f1);
    }
  }

  /**
   * This {@link CoGroupFunction} accumulates all properties that might be send to a vertex and
   * stores them in a {@link PropertyValue} list.
   */
  private static class AccumulatePropagatedValues
    implements CoGroupFunction<Tuple2<GradoopId, PropertyValue>, Vertex, Vertex> {

    /**
     * The property key where the PropertyValue list should be stored at the target vertices.
     */
    private final String targetVertexPropertyKey;

    /**
     * The constructor of the co group function for accumulation of collected property values.
     *
     * @param targetVertexPropertyKey The property key where the PropertyValue list should be
     *                                stored at the target vertices.
     */
    AccumulatePropagatedValues(String targetVertexPropertyKey) {
      this.targetVertexPropertyKey = targetVertexPropertyKey;
    }

    @Override
    public void coGroup(Iterable<Tuple2<GradoopId, PropertyValue>> propertyValues,
                        Iterable<Vertex> vertices, Collector<Vertex> out) {
      // should only contain one vertex, based on the uniqueness of gradoop ids
      Vertex v = vertices.iterator().next();

      // collect values of neighbors
      List<PropertyValue> values = new ArrayList<>();
      propertyValues.forEach(t -> values.add(t.f1));

      // add to vertex
      PropertyValue pv = new PropertyValue();
      pv.setList(values);
      v.setProperty(targetVertexPropertyKey, pv);

      out.collect(v);
    }
  }
}
