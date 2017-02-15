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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperEdgeGroupItem;

import java.util.List;

/**
 * Creates a minimal representation of edge data to be used for grouping.
 *
 * The output of that mapper is {@link SuperEdgeGroupItem} that contains
 * the edge id, edge label, edge source, edge target, edge group properties and edge aggregate
 * properties.
 */
//@FunctionAnnotation.ForwardedFields("id->f0")
//@FunctionAnnotation.ReadFields("label;properties") //TODO check for updates (source,target)
public class BuildSuperEdgeGroupItem
  extends BuildBase
  implements MapFunction<Edge, SuperEdgeGroupItem> {

  /**
   * Reduce object instantiations.
   */
  private final SuperEdgeGroupItem reuseSuperEdgeGroupItem;

  /**
   * Creates map function
   *
   * @param groupPropertyKeys edge property keys
   * @param useLabel          true, if label shall be considered
   * @param edgeAggregators aggregate functions for super edges
   */
  public BuildSuperEdgeGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> edgeAggregators) {
    super(groupPropertyKeys, useLabel, edgeAggregators);

    this.reuseSuperEdgeGroupItem = new SuperEdgeGroupItem();
    this.reuseSuperEdgeGroupItem.setSuperEdgeId(GradoopId.NULL_VALUE);
    this.reuseSuperEdgeGroupItem.setSuperEdge(false);
    if (!doAggregate()) {
      this.reuseSuperEdgeGroupItem.setAggregateValues(
        PropertyValueList.createEmptyList());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SuperEdgeGroupItem map(Edge edge) throws Exception {
    reuseSuperEdgeGroupItem.setEdgeId(edge.getId());
    reuseSuperEdgeGroupItem.setSourceId(edge.getSourceId());
    reuseSuperEdgeGroupItem.setTargetId(edge.getTargetId());
    reuseSuperEdgeGroupItem.setGroupLabel(getLabel(edge));
    reuseSuperEdgeGroupItem.setGroupingValues(getGroupProperties(edge));
    if (doAggregate()) {
      reuseSuperEdgeGroupItem.setAggregateValues(getAggregateValues(edge));
    }
    return reuseSuperEdgeGroupItem;
  }
}
