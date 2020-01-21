/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.dataintegration.transformation.functions.CreateMappingFromMarkedDuplicates;
import org.gradoop.dataintegration.transformation.functions.GetPropertiesAsList;
import org.gradoop.dataintegration.transformation.functions.MarkDuplicatesInGroup;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.functions.epgm.EdgeSourceUpdateJoin;
import org.gradoop.flink.model.impl.functions.epgm.EdgeTargetUpdateJoin;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;

import java.util.List;
import java.util.Objects;

/**
 * Deduplicates vertices based on some attribute.<p>
 * Given a label and a list of property keys, this operator will find all vertices of that label and condense
 * duplicates to one vertex. Two (or more) vertices are considered duplicates if all property values,
 * respective to the list of given keys, are equal.<p>
 * Other attributes of the vertices are ignored, the new (condensed) vertex will have the attributes of some
 * (random) vertex of its duplicates.
 * <p>
 * This will create no new elements and retain the graph head.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class VertexDeduplication<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * The label of vertices to deduplicate.
   */
  private final String label;

  /**
   * The list of property keys used to determine equal vertices.
   */
  private final List<String> propertyKeys;

  /**
   * Creates a new instance of this vertex deduplication operator.
   *
   * @param label        The label of vertices to deduplicate.
   * @param propertyKeys The properties used to check if two (or more) vertices of that label are duplicates.
   */
  public VertexDeduplication(String label, List<String> propertyKeys) {
    this.label = Objects.requireNonNull(label);
    this.propertyKeys = Objects.requireNonNull(propertyKeys);
  }

  @Override
  public LG execute(LG graph) {
    DataSet<V> annotatedVertices = graph.getVerticesByLabel(label)
      .groupBy(new GetPropertiesAsList<>(propertyKeys))
      .reduceGroup(new MarkDuplicatesInGroup<>());
    DataSet<Tuple2<GradoopId, GradoopId>> vertexToDedupVertex = annotatedVertices
      .flatMap(new CreateMappingFromMarkedDuplicates<>());
    DataSet<V> deduplicatedVertices = annotatedVertices
      .filter(new ByProperty<V>(MarkDuplicatesInGroup.PROPERTY_KEY).negate());
    DataSet<V> otherVertices = graph.getVertices().filter(new ByLabel<V>(label).negate());
    DataSet<E> updatesEdges = graph.getEdges()
      .leftOuterJoin(vertexToDedupVertex)
      .where(new SourceId<>())
      .equalTo(0)
      .with(new EdgeSourceUpdateJoin<>())
      .leftOuterJoin(vertexToDedupVertex)
      .where(new TargetId<>())
      .equalTo(0)
      .with(new EdgeTargetUpdateJoin<>());

    return graph.getFactory().fromDataSets(graph.getGraphHead(),
      otherVertices.union(deduplicatedVertices),
      updatesEdges);
  }
}
