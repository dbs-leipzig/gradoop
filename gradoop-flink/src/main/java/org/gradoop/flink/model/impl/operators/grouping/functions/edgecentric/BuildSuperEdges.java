/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
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

    Edge superEdge = edgeFactory.initEdge(superEdgeGroupItem.getEdgeId(), sourceId, targetId);

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
