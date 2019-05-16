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
package org.gradoop.flink.model.impl.operators.grouping;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.filters.And;
import org.gradoop.flink.model.impl.functions.filters.Not;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

import java.util.List;
import java.util.Optional;

/**
 * Grouping implementation that uses group + groupReduce for building super
 * vertices and updating the original vertices.
 * <p>
 * Algorithmic idea:
 * <p>
 * 1) Map vertices to a minimal representation, i.e. {@link VertexGroupItem}.
 * 2) Group vertices on label and/or property.
 * 3) Create a super vertex id for each group and collect a non-candidate
 * {@link VertexGroupItem} for each group element and one additional
 * super vertex tuple that holds the group aggregate.
 * 4) Filter output of 3)
 * a) non-candidate tuples are mapped to {@link VertexWithSuperVertex}
 * b) super vertex tuples are used to build final super vertices
 * 5) Map edges to a minimal representation, i.e. {@link EdgeGroupItem}
 * 6) Join edges with output of 4a) and replace source/target id with super
 * vertex id.
 * 7) Updated edges are grouped by source and target id and optionally by label
 * and/or edge property.
 * 8) Group combine on the workers and compute aggregate.
 * 9) Group reduce globally and create final super edges.
 */
public class GroupingGroupReduce extends Grouping {

  /**
   * vertices are kept, iff their label is empty and when they don't have any property that is
   * grouped by
   */
  private final FilterFunction<Vertex>
    verticesWithoutGroupFilter = new And<>(new EmptyLabelFilter(),
    new Not<>(new HasLabelingGroupPropertyFilter(getDefaultVertexLabelGroup())));

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels             group on vertex label true/false
   * @param useEdgeLabels               group on edge label true/false
   * @param vertexLabelGroups           stores grouping properties for vertex labels
   * @param edgeLabelGroups             stores grouping properties for edge labels
   * @param retainVerticesWithoutGroups convert vertices that are not member of a group as is to
   *                                    supervertices
   * @param defaultVertexLabelGroup     default vertex label group
   */
  GroupingGroupReduce(boolean useVertexLabels, boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups, List<LabelGroup> edgeLabelGroups,
    boolean retainVerticesWithoutGroups,
    LabelGroup defaultVertexLabelGroup) {
    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups,
      retainVerticesWithoutGroups,
      defaultVertexLabelGroup);

  }

  @Override
  protected LogicalGraph groupInternal(LogicalGraph graph) {

    DataSet<Vertex> vertices = graph.getVertices();

    Optional<DataSet<Vertex>> optionalConvertedVertices =
      isKeepingVertices() ? Optional.of(getVerticesWithoutGroup(vertices)) : Optional.empty();

    if (optionalConvertedVertices.isPresent()) {
      vertices = getVerticesToGroup(vertices);
    }

    DataSet<VertexGroupItem> verticesForGrouping = vertices.flatMap(
      new BuildVertexGroupItem(useVertexLabels(), getVertexLabelGroups(), false));

    // group vertices by label / properties / both
    DataSet<VertexGroupItem> vertexGroupItems = groupVertices(verticesForGrouping)
      // apply aggregate function
      .reduceGroup(new ReduceVertexGroupItems(useVertexLabels()));

    DataSet<Vertex> superVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterSuperVertices())
      // build super vertices
      .map(new BuildSuperVertex(useVertexLabels(), config.getVertexFactory()));

    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap = vertexGroupItems
      // filter group element tuples
      .filter(new FilterRegularVertices())
      // build vertex to group representative tuple
      .map(new BuildVertexWithSuperVertex());


    if (optionalConvertedVertices.isPresent()) {

      DataSet<Vertex> convertedVertices = optionalConvertedVertices.get();

      // --- TODO property and aggregate step necessary?
      // Add all properties that were grouped by as null values to converted vertices
      convertedVertices =
        convertedVertices.map(new AddPropertiesAsNullValues(getDefaultVertexLabelGroup()));

      // Execute aggregation functions once for each converted vertex
      convertedVertices =
        convertedVertices.map(new AddSingleAggregationResult(getDefaultVertexLabelGroup()));
      // ---

      superVertices = superVertices.union(convertedVertices);

      DataSet<VertexWithSuperVertex> optionalRepresentatives =
        convertedVertices.map(new VertexSuperVertexIdentity());

      vertexToRepresentativeMap = vertexToRepresentativeMap.union(optionalRepresentatives);

    }

    // build super edges
    DataSet<Edge> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return config.getLogicalGraphFactory().fromDataSets(superVertices, superEdges);
  }

  //TODO extract super function
  private DataSet<Vertex> getVerticesToGroup(DataSet<Vertex> vertices) {
    return vertices.filter(new Not<>(verticesWithoutGroupFilter));
  }


  private DataSet<Vertex> getVerticesWithoutGroup(DataSet<Vertex> vertices) {
    return vertices.filter(verticesWithoutGroupFilter);
  }

  /**
   * Returns true, if a vertice's label is empty.
   */
  private static class EmptyLabelFilter implements FilterFunction<Vertex> {

    @Override
    public boolean filter(Vertex value) throws Exception {
      return value.getLabel().isEmpty();
    }
  }

  /**
   * Returns true, if a vertice has property keys of a labelGroup.
   */
  private static class HasLabelingGroupPropertyFilter implements FilterFunction<Vertex> {

    private final LabelGroup labelGroup;

    public HasLabelingGroupPropertyFilter(LabelGroup labelGroup) {
      this.labelGroup = labelGroup;
    }

    @Override
    public boolean filter(Vertex value) throws Exception {
      return labelGroup.getPropertyKeys()
        .parallelStream()
        .anyMatch(value::hasProperty);
    }
  }


  /**
   * Creates a {@link VertexWithSuperVertex} with both components referencing the same
   * vertex that is mapped on.
   */
  private class VertexSuperVertexIdentity implements MapFunction<Vertex, VertexWithSuperVertex> {

    /**
     * Avoid object instantiation.
     */
    private final VertexWithSuperVertex reuseTuple;

    private VertexSuperVertexIdentity() {
      reuseTuple = new VertexWithSuperVertex();
    }

    @Override
    public VertexWithSuperVertex map(Vertex value) throws Exception {
      reuseTuple.setVertexId(value.getId());
      reuseTuple.setSuperVertexId((value.getId()));
      return reuseTuple;
    }

  }

  /**
   * Adds all properties of a {@link LabelGroup} to a vertex, initialized with
   * PropertyValue.NULL_VALUE.
   */
  private class AddPropertiesAsNullValues implements MapFunction<Vertex, Vertex> {

    private final LabelGroup labelGroup;

    public AddPropertiesAsNullValues(LabelGroup labelGroup) {
      this.labelGroup = labelGroup;
    }

    @Override
    public Vertex map(Vertex value) throws Exception {

      labelGroup.getPropertyKeys()
        .forEach(key -> value.setProperty(key, PropertyValue.NULL_VALUE));

      return value;
    }
  }

  /**
   * Executes each aggregation function in a labelGroup once and adds the result as a property to
   * a vertex.
   */
  private class AddSingleAggregationResult implements MapFunction<Vertex, Vertex> {

    private final LabelGroup labelGroup;

    public AddSingleAggregationResult(LabelGroup labelGroup) {

      this.labelGroup = labelGroup;
    }

    @Override
    public Vertex map(Vertex value) throws Exception {

      labelGroup.getAggregateFunctions()
        .forEach(fun -> value.setProperty(fun.getAggregatePropertyKey(), fun.getIncrement(value)));

      return value;
    }
  }
}
