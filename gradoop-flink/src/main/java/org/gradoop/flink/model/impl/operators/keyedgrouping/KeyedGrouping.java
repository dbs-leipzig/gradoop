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
package org.gradoop.flink.model.impl.operators.keyedgrouping;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.BuildSuperEdgeFromTuple;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.BuildSuperVertexFromTuple;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.BuildTuplesFromEdges;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.BuildTuplesFromVertices;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.GroupingConstants;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.ReduceEdgeTuples;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.ReduceVertexTuples;
import org.gradoop.flink.model.impl.operators.keyedgrouping.functions.UpdateIdField;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A grouping operator similar to {@link org.gradoop.flink.model.impl.operators.grouping.Grouping}
 * that uses key functions to determine grouping keys.
 *
 * @param <G> The graph head type.
 * @param <V> The vertex type.
 * @param <E> The edge type.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public class KeyedGrouping<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * The vertex grouping keys.
   */
  private final List<KeyFunction<V, ?>> vertexGroupingKeys;

  /**
   * The vertex aggregate functions.
   */
  private final List<AggregateFunction> vertexAggregateFunctions;

  /**
   * The edge grouping keys.
   */
  private final List<KeyFunction<E, ?>> edgeGroupingKeys;

  /**
   * The edge aggregate functions.
   */
  private final List<AggregateFunction> edgeAggregateFunctions;

  /**
   * Should a combine step be used before grouping? Note that this currently only affects edges.
   */
  private boolean useGroupCombine = true;

  /**
   * Instantiate this grouping function.
   *
   * @param vertexGroupingKeys       The vertex grouping keys.
   * @param vertexAggregateFunctions The vertex aggregate functions.
   * @param edgeGroupingKeys         The edge grouping keys.
   * @param edgeAggregateFunctions   The edge aggregate functions.
   * @implNote Label-specific grouping is not supported by this implementation.
   */
  public KeyedGrouping(List<KeyFunction<V, ?>> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<KeyFunction<E, ?>> edgeGroupingKeys,
    List<AggregateFunction> edgeAggregateFunctions) {
    this.vertexGroupingKeys = Objects.requireNonNull(vertexGroupingKeys);
    this.vertexAggregateFunctions = vertexAggregateFunctions == null ? Collections.emptyList() :
      vertexAggregateFunctions;
    this.edgeGroupingKeys = edgeGroupingKeys == null ? Collections.emptyList() :
      edgeGroupingKeys;
    this.edgeAggregateFunctions = edgeAggregateFunctions == null ? Collections.emptyList() :
      edgeAggregateFunctions;
  }

  @Override
  public LG execute(LG graph) {
    if (vertexGroupingKeys.isEmpty() && edgeGroupingKeys.isEmpty()) {
      return graph;
    }
    DataSet<Tuple> verticesWithSuperVertex = graph.getVertices()
      .map(new BuildTuplesFromVertices<>(vertexGroupingKeys, vertexAggregateFunctions))
      .groupBy(getInternalVertexGroupingKeys())
      .reduceGroup(new ReduceVertexTuples<>(
        GroupingConstants.VERTEX_TUPLE_RESERVED + vertexGroupingKeys.size(), vertexAggregateFunctions))
      .withForwardedFields(getInternalForwardedFieldsForVertexReduce());
    DataSet<Tuple2<GradoopId, GradoopId>> idToSuperId =
      verticesWithSuperVertex.project(
        GroupingConstants.VERTEX_TUPLE_ID, GroupingConstants.VERTEX_TUPLE_SUPERID);

    DataSet<Tuple> edgesWithUpdatedIds = graph.getEdges()
      .map(new BuildTuplesFromEdges<>(edgeGroupingKeys, edgeAggregateFunctions))
      .join(idToSuperId)
      .where(GroupingConstants.EDGE_TUPLE_SOURCEID)
      .equalTo(GroupingConstants.VERTEX_TUPLE_ID)
      .with(new UpdateIdField<>(GroupingConstants.EDGE_TUPLE_SOURCEID))
      .withForwardedFieldsFirst(getInternalForwardedFieldsForEdgeUpdate(
        GroupingConstants.EDGE_TUPLE_SOURCEID, false))
      .withForwardedFieldsSecond(getInternalForwardedFieldsForEdgeUpdate(
        GroupingConstants.EDGE_TUPLE_SOURCEID, true))
      .join(idToSuperId)
      .where(GroupingConstants.EDGE_TUPLE_TARGETID)
      .equalTo(GroupingConstants.VERTEX_TUPLE_ID)
      .with(new UpdateIdField<>(GroupingConstants.EDGE_TUPLE_TARGETID))
      .withForwardedFieldsFirst(getInternalForwardedFieldsForEdgeUpdate(
        GroupingConstants.EDGE_TUPLE_TARGETID, false))
      .withForwardedFieldsSecond(getInternalForwardedFieldsForEdgeUpdate(
        GroupingConstants.EDGE_TUPLE_TARGETID, true));

    DataSet<Tuple> superEdgeTuples = edgesWithUpdatedIds
      .groupBy(getInternalEdgeGroupingKeys())
      .reduceGroup(new ReduceEdgeTuples<>(
        GroupingConstants.EDGE_TUPLE_RESERVED + edgeGroupingKeys.size(), edgeAggregateFunctions))
      .setCombinable(useGroupCombine)
      .withForwardedFields(getInternalForwardedFieldsForEdgeReduce());

    DataSet<V> superVertices = verticesWithSuperVertex
      .filter(new FilterSuperVertices<>())
      .map(new BuildSuperVertexFromTuple<>(vertexGroupingKeys, vertexAggregateFunctions,
        graph.getFactory().getVertexFactory()));

    DataSet<E> superEdges = superEdgeTuples
      .map(new BuildSuperEdgeFromTuple<>(edgeGroupingKeys, edgeAggregateFunctions,
        graph.getFactory().getEdgeFactory()));

    return graph.getFactory().fromDataSets(superVertices, superEdges);
  }

  /**
   * Get the internal representation of the forwarded fields for the {@link ReduceEdgeTuples} step.
   * The forwarded fields for this step will be the grouping keys.
   *
   * @return A string containing the field names of all forwarded fields.
   */
  private String getInternalForwardedFieldsForEdgeReduce() {
    return IntStream.of(getInternalEdgeGroupingKeys()).mapToObj(i -> "f" + i)
      .collect(Collectors.joining(";"));
  }

  /**
   * Get the internal representation of the forwarded fields for the {@link ReduceVertexTuples} step.
   * The forwarded fields for this step will be the grouping keys.
   *
   * @return A string containing the field names of all forwarded fields.
   */
  private String getInternalForwardedFieldsForVertexReduce() {
    return IntStream.of(getInternalVertexGroupingKeys()).mapToObj(i -> "f" + i)
      .collect(Collectors.joining(";"));
  }

  /**
   * Get the internal representation of the forwarded fields for the {@link UpdateIdField} steps.
   * Forwarded fields will be all but the currently updated field for the left side and the new id for
   * the right side.
   *
   * @param targetField The index of the updated field.
   * @param fromSecond  Return the forwarded fields for the right side of the join if set to true (returns
   *                    the fields for the left side otherwise).
   * @return The string containing the forwarded fields info.
   */
  private String getInternalForwardedFieldsForEdgeUpdate(int targetField, boolean fromSecond) {
    if (fromSecond) {
      return "f1->f" + targetField;
    } else {
      return IntStream.range(0,
        GroupingConstants.EDGE_TUPLE_RESERVED + edgeGroupingKeys.size() + edgeAggregateFunctions.size())
        .filter(i -> i != targetField).mapToObj(i -> "f" + i).collect(Collectors.joining(";"));
    }
  }

  /**
   * Get the internal grouping keys used for grouping the edge tuples.
   *
   * @return The grouping keys, as tuple indices.
   */
  private int[] getInternalEdgeGroupingKeys() {
    return IntStream.range(0, GroupingConstants.EDGE_TUPLE_RESERVED + edgeGroupingKeys.size())
      .toArray();
  }

  /**
   * Get the internal grouping keys used for grouping the vertex tuples.
   *
   * @return The grouping keys, as tuple indices.
   */
  private int[] getInternalVertexGroupingKeys() {
    return IntStream.range(GroupingConstants.VERTEX_TUPLE_RESERVED,
      GroupingConstants.VERTEX_TUPLE_RESERVED + vertexGroupingKeys.size()).toArray();
  }

  /**
   * Enable or disable an optional combine step before the reduce step.
   * Note that this currently only affects the edge reduce step.
   * <p>
   * The combine step is enabled by default.
   *
   * @param useGroupCombine {@code true}, if a combine step should be used.
   * @return This operator.
   */
  public KeyedGrouping<G, V, E, LG, GC> setUseGroupCombine(boolean useGroupCombine) {
    this.useGroupCombine = useGroupCombine;
    return this;
  }
}
