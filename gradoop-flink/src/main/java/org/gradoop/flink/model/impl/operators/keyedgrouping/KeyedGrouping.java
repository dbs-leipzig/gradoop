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
import org.gradoop.flink.model.impl.functions.filters.Not;
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
import java.util.stream.IntStream;

/**
 * Group a graph based on some key functions.<p>
 * This operator is initialized with {@link List lists} of {@link KeyFunction key functions} and
 * {@link AggregateFunction aggregate functions}. The output of this operator is the grouped graph
 * (also called summarized graph or summary graph) which is calculated by reducing similar vertices and
 * edges to single elements, called super-vertices and -edges respectively. Elements are considered similar
 * if the values of the key functions are all equal.<p>
 * The aggregate functions will summarize certain property values of similar elements and store the result
 * on the super-elements.<p>
 * This implementation will use tuples to represent elements during the execution of this operator.
 * These tuples will contain IDs, the values of key functions and the aggregate values for each element
 * and super-element.
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
   */
  public KeyedGrouping(List<KeyFunction<V, ?>> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<KeyFunction<E, ?>> edgeGroupingKeys,
    List<AggregateFunction> edgeAggregateFunctions) {
    if (vertexGroupingKeys == null || vertexGroupingKeys.isEmpty()) {
      // Grouping with no keys is not supported, we therefore add a custom key function that returns a
      // constant as a pseudo-key for every element.
      this.vertexGroupingKeys = Collections.singletonList(GroupingKeys.nothing());
    } else {
      this.vertexGroupingKeys = vertexGroupingKeys;
    }
    this.vertexAggregateFunctions = vertexAggregateFunctions == null ? Collections.emptyList() :
      vertexAggregateFunctions;
    this.edgeGroupingKeys = edgeGroupingKeys == null ? Collections.emptyList() :
      edgeGroupingKeys;
    this.edgeAggregateFunctions = edgeAggregateFunctions == null ? Collections.emptyList() :
      edgeAggregateFunctions;
  }

  @Override
  public LG execute(LG graph) {
    /* First we create tuple representations of each vertex.
       Those tuples will then be grouped by the respective key fields (the fields containing the values
       extracted by the key functions) and reduced to assign a super vertex and to calculate aggregates. */
    DataSet<Tuple> verticesWithSuperVertex = graph.getVertices()
      .map(new BuildTuplesFromVertices<>(vertexGroupingKeys, vertexAggregateFunctions))
      .groupBy(getInternalVertexGroupingKeys())
      .reduceGroup(new ReduceVertexTuples<>(
        GroupingConstants.VERTEX_TUPLE_RESERVED + vertexGroupingKeys.size(), vertexAggregateFunctions));
    /* Extract a mapping from vertex-ID to super-vertex-ID from the result of the vertex-reduce step. */
    DataSet<Tuple2<GradoopId, GradoopId>> idToSuperId = verticesWithSuperVertex
      .filter(new Not<>(new FilterSuperVertices<>()))
      .project(GroupingConstants.VERTEX_TUPLE_ID, GroupingConstants.VERTEX_TUPLE_SUPERID);

    /* Create tuple representations of each edge and update the source- and target-ids of those tuples with
       with the mapping extracted in the previous step. Edges will then point from and to super-vertices. */
    DataSet<Tuple> edgesWithUpdatedIds = graph.getEdges()
      .map(new BuildTuplesFromEdges<>(edgeGroupingKeys, edgeAggregateFunctions))
      .join(idToSuperId)
      .where(GroupingConstants.EDGE_TUPLE_SOURCEID)
      .equalTo(GroupingConstants.VERTEX_TUPLE_ID)
      .with(new UpdateIdField<>(GroupingConstants.EDGE_TUPLE_SOURCEID))
      .join(idToSuperId)
      .where(GroupingConstants.EDGE_TUPLE_TARGETID)
      .equalTo(GroupingConstants.VERTEX_TUPLE_ID)
      .with(new UpdateIdField<>(GroupingConstants.EDGE_TUPLE_TARGETID));

    /* Group the edge-tuples by the key fields and vertex IDs and reduce them to single elements. */
    DataSet<Tuple> superEdgeTuples = edgesWithUpdatedIds
      .groupBy(getInternalEdgeGroupingKeys())
      .reduceGroup(new ReduceEdgeTuples<>(
        GroupingConstants.EDGE_TUPLE_RESERVED + edgeGroupingKeys.size(), edgeAggregateFunctions))
      .setCombinable(useGroupCombine);

    /* Rebuild super-vertices from vertex-tuples. Those new vertices contain the data extracted by the key
       functions and aggregated by the aggregate functions. */
    DataSet<V> superVertices = verticesWithSuperVertex
      .filter(new FilterSuperVertices<>())
      .map(new BuildSuperVertexFromTuple<>(vertexGroupingKeys, vertexAggregateFunctions,
        graph.getFactory().getVertexFactory()));

    /* Rebuild super-edges from edge-tuples. */
    DataSet<E> superEdges = superEdgeTuples
      .map(new BuildSuperEdgeFromTuple<>(edgeGroupingKeys, edgeAggregateFunctions,
        graph.getFactory().getEdgeFactory()));

    return graph.getFactory().fromDataSets(superVertices, superEdges);
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
