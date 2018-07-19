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
package org.gradoop.flink.model.impl.operators.grouping;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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

/**
 * Grouping implementation that uses group + groupReduce for building super
 * vertices and updating the original vertices.
 *
 * Algorithmic idea:
 *
 * 1) Map vertices to a minimal representation, i.e. {@link VertexGroupItem}.
 * 2) Group vertices on label and/or property.
 * 3) Create a super vertex id for each group and collect a non-candidate
 *    {@link VertexGroupItem} for each group element and one additional
 *    super vertex tuple that holds the group aggregate.
 * 4) Filter output of 3)
 *    a) non-candidate tuples are mapped to {@link VertexWithSuperVertex}
 *    b) super vertex tuples are used to build final super vertices
 * 5) Map edges to a minimal representation, i.e. {@link EdgeGroupItem}
 * 6) Join edges with output of 4a) and replace source/target id with super
 *    vertex id.
 * 7) Updated edges are grouped by source and target id and optionally by label
 *    and/or edge property.
 * 8) Group combine on the workers and compute aggregate.
 * 9) Group reduce globally and create final super edges.
 */
public class GroupingGroupReduce extends Grouping {
  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels   group on vertex label true/false
   * @param useEdgeLabels     group on edge label true/false
   * @param vertexLabelGroups stores grouping properties for vertex labels
   * @param edgeLabelGroups   stores grouping properties for edge labels
   */
  GroupingGroupReduce(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups) {
    super(
      useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph groupInternal(LogicalGraph graph) {

    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      // map vertex to vertex group item
      .flatMap(new BuildVertexGroupItem(useVertexLabels(), getVertexLabelGroups()));

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

    // build super edges
    DataSet<Edge> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return config.getLogicalGraphFactory().fromDataSets(superVertices, superEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return GroupingGroupReduce.class.getName();
  }
}
