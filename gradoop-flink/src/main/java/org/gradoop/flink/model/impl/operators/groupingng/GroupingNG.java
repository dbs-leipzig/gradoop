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
package org.gradoop.flink.model.impl.operators.groupingng;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.functions.GroupingKeyFunction;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.groupingng.functions.BuildSuperEdgeFromTuple;
import org.gradoop.flink.model.impl.operators.groupingng.functions.BuildSuperVertexFromTuple;
import org.gradoop.flink.model.impl.operators.groupingng.functions.BuildTuplesFromEdges;
import org.gradoop.flink.model.impl.operators.groupingng.functions.ReduceEdgeTuples;
import org.gradoop.flink.model.impl.operators.groupingng.functions.ReduceVertexTuples;
import org.gradoop.flink.model.impl.operators.groupingng.functions.GroupingNGConstants;
import org.gradoop.flink.model.impl.operators.groupingng.functions.UpdateIdField;
import org.gradoop.flink.model.impl.operators.groupingng.functions.BuildTuplesFromVertices;
import org.gradoop.flink.model.impl.operators.groupingng.functions.FilterSuperVertices;

import java.util.ArrayList;
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
public class GroupingNG<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * The vertex grouping keys.
   */
  private final List<GroupingKeyFunction<V, ?>> vertexGroupingKeys;

  /**
   * The vertex aggregate functions.
   */
  private final List<AggregateFunction> vertexAggregateFunctions;

  /**
   * The edge grouping keys.
   */
  private final List<GroupingKeyFunction<E, ?>> edgeGroupingKeys;

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
  public GroupingNG(List<GroupingKeyFunction<V, ?>> vertexGroupingKeys,
    List<AggregateFunction> vertexAggregateFunctions,
    List<GroupingKeyFunction<E, ?>> edgeGroupingKeys,
    List<AggregateFunction> edgeAggregateFunctions) {
    this.vertexGroupingKeys = Objects.requireNonNull(vertexGroupingKeys);
    this.vertexAggregateFunctions = vertexAggregateFunctions == null ? Collections.emptyList() :
      vertexAggregateFunctions;
    this.edgeGroupingKeys = edgeGroupingKeys == null ? Collections.emptyList() :
      edgeGroupingKeys;
    this.edgeAggregateFunctions = edgeAggregateFunctions == null ? Collections.emptyList() :
      edgeAggregateFunctions;
  }

  /**
   * Instantiate this grouping function.<p>
   * <b>Hint:</b> This constructor is only used for compatibility with the old grouping API. It is
   * advised to use {@link #GroupingNG(List, List, List, List)} instead.<p>
   * <b>Warning:</b> Label-specific grouping is not (yet) supported by this grouping implementation.
   * An {@link UnsupportedOperationException} will be thrown when any label group other than the
   * default label groups is given.
   *
   * @param useVertexLabels  Group by vertex labels.
   * @param useEdgeLabels    Group by edge labels.
   * @param vertexLabelGroup The default vertex label group.
   * @param edgeLabelGroup   The default edge label group.
   */
  public GroupingNG(boolean useVertexLabels, boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroup, List<LabelGroup> edgeLabelGroup) {
    this(asKeyFunctions(useVertexLabels, vertexLabelGroup), asAggregateFunctions(vertexLabelGroup),
      asKeyFunctions(useEdgeLabels, edgeLabelGroup), asAggregateFunctions(edgeLabelGroup));
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
        GroupingNGConstants.VERTEX_TUPLE_RESERVED + vertexGroupingKeys.size(), vertexAggregateFunctions))
      .withForwardedFields(getInternalForwardedFieldsForVertexReduce());
    DataSet<Tuple2<GradoopId, GradoopId>> idToSuperId =
      verticesWithSuperVertex.project(
        GroupingNGConstants.VERTEX_TUPLE_ID, GroupingNGConstants.VERTEX_TUPLE_SUPERID);

    DataSet<Tuple> edgesWithUpdatedIds = graph.getEdges()
      .map(new BuildTuplesFromEdges<>(edgeGroupingKeys, edgeAggregateFunctions))
      .join(idToSuperId)
      .where(GroupingNGConstants.EDGE_TUPLE_SOURCEID)
      .equalTo(GroupingNGConstants.VERTEX_TUPLE_ID)
      .with(new UpdateIdField<>(GroupingNGConstants.EDGE_TUPLE_SOURCEID))
      .join(idToSuperId)
      .where(GroupingNGConstants.EDGE_TUPLE_TARGETID)
      .equalTo(GroupingNGConstants.VERTEX_TUPLE_ID)
      .with(new UpdateIdField<>(GroupingNGConstants.EDGE_TUPLE_TARGETID));

    DataSet<Tuple> superEdgeTuples = edgesWithUpdatedIds
      .groupBy(getInternalEdgeGroupingKeys())
      .reduceGroup(new ReduceEdgeTuples<>(
        GroupingNGConstants.EDGE_TUPLE_RESERVED + edgeGroupingKeys.size(), edgeAggregateFunctions))
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
   * Get the internal grouping keys used for grouping the edge tuples.
   *
   * @return The grouping keys, as tuple indices.
   */
  private int[] getInternalEdgeGroupingKeys() {
    return IntStream.range(0, GroupingNGConstants.EDGE_TUPLE_RESERVED + edgeGroupingKeys.size())
      .toArray();
  }

  /**
   * Get the internal grouping keys used for grouping the vertex tuples.
   *
   * @return The grouping keys, as tuple indices.
   */
  private int[] getInternalVertexGroupingKeys() {
    return IntStream.range(GroupingNGConstants.VERTEX_TUPLE_RESERVED,
      GroupingNGConstants.VERTEX_TUPLE_RESERVED + vertexGroupingKeys.size()).toArray();
  }

  /**
   * For compatibility reasons only: Convert label groups to aggregate functions.
   *
   * @param labelGroups The label groups to convert. (Only the default group is supported.)
   * @return Aggregate functions corresponding to those groups.
   */
  private static List<AggregateFunction> asAggregateFunctions(List<LabelGroup> labelGroups) {
    return getDefaultGroup(labelGroups).getAggregateFunctions();
  }

  /**
   * For compatibility reasons only: Convert label groups to key functions.
   *
   * @param useLabels   Should labels be used for grouping?
   * @param labelGroups The label groups to convert. (Only the default group is supported.)
   * @param <T> The element type for the grouping key function.
   * @return Key functions corresponding to those groups.
   */
  private static <T extends Element> List<GroupingKeyFunction<T, ?>> asKeyFunctions(
    boolean useLabels, List<LabelGroup> labelGroups) {
    List<GroupingKeyFunction<T, ?>> keyFunctions = new ArrayList<>();
    if (useLabels) {
      keyFunctions.add(GroupingKeys.label());
    }
    getDefaultGroup(labelGroups).getPropertyKeys()
      .forEach(k -> keyFunctions.add(GroupingKeys.property(k)));
    return keyFunctions;
  }

  /**
   * For compatibility reasons only: Get the default label group or throw an exception.
   *
   * @param labelGroups A list of label groups.
   * @return The default label group, if it is the only label group.
   */
  private static LabelGroup getDefaultGroup(List<LabelGroup> labelGroups) {
    if (labelGroups.size() != 1) {
      throw new UnsupportedOperationException(
        "Label specific grouping is not supported by this implementation.");
    } else {
      LabelGroup labelGroup = labelGroups.get(0);
      if (!(labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_EDGE_LABEL_GROUP) ||
        labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP))) {
        throw new UnsupportedOperationException("Label specific grouping is not supported",
          new IllegalArgumentException(
            "The list of label groups does not contain the default label group."));
      }
      return labelGroup;
    }
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
  public GroupingNG<G, V, E, LG, GC> setUseGroupCombine(boolean useGroupCombine) {
    this.useGroupCombine = useGroupCombine;
    return this;
  }
}
