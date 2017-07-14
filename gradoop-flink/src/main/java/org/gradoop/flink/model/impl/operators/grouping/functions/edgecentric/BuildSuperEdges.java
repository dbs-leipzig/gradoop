/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;


/**
 * Creates a new super edge representing an edge group. The edge stores the
 * group label, the group property value and the aggregate values for its group.
 */
public class BuildSuperEdges
  extends BuildBase
  implements CoGroupFunction<
    SuperEdgeGroupItem, SuperVertexGroupItem, Edge>, ResultTypeQueryable<Edge> {

  /**
   * Edge edgeFactory.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Creates map function.
   *
   * @param useLabel true, if vertex label shall be considered
   * @param edgeFactory edge factory
   */
  public BuildSuperEdges(boolean useLabel, EdgeFactory edgeFactory) {
    super(useLabel);
    this.edgeFactory = edgeFactory;
  }


  @Override
  public void coGroup(Iterable<SuperEdgeGroupItem> superEdgeGroupItems,
    Iterable<SuperVertexGroupItem> vertexWithSuperVertexAndEdges, Collector<Edge> collector) throws
    Exception {

    // only one edge per id
    SuperEdgeGroupItem superEdgeGroupItem = superEdgeGroupItems.iterator().next();

    GradoopId sourceId = null;
    GradoopId targetId = null;

    // set the correct super vertex id as source or target id
    for (SuperVertexGroupItem superVertexGroupItem : vertexWithSuperVertexAndEdges) {
      // check if the collection of source/target ids of vertices of the edge group item is equal
      // to the one represented by the super vertex item and take its id
      if (superVertexGroupItem.f0.equals(superEdgeGroupItem.getSourceIds())) {
        sourceId = superVertexGroupItem.f1;
      } else if (superVertexGroupItem.f0.equals(superEdgeGroupItem.getTargetIds())) {
        targetId = superVertexGroupItem.f1;
      }
    }

    Edge superEdge = edgeFactory.initEdge(superEdgeGroupItem.getSuperEdgeId(), sourceId, targetId);

    superEdge.setLabel(superEdgeGroupItem.getGroupLabel());
    setGroupProperties(
      superEdge,
      superEdgeGroupItem.getGroupingValues(),
      superEdgeGroupItem.getLabelGroup());
    setAggregateValues(
      superEdge,
      superEdgeGroupItem.getAggregateValues(),
      superEdgeGroupItem.getLabelGroup().getAggregators());

    collector.collect(superEdge);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<Edge> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
