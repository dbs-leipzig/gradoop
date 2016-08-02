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
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.common.model.api.epgm.Edge;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.util.List;

/**
 * Takes an EPGM edge as input and creates a {@link EdgeGroupItem} which
 * contains only necessary information for further processing.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1;")
@FunctionAnnotation.ReadFields("label;properties")
public class BuildEdgeGroupItem<E extends Edge>
  extends BuildBase
  implements MapFunction<E, EdgeGroupItem> {

  /**
   * Avoid object initialization in each call.
   */
  private final EdgeGroupItem reuseEdgeGroupItem;

  /**
   * Creates map function.
   *
   * @param groupPropertyKeys vertex property key for grouping
   * @param useLabel          true, if vertex label shall be used
   * @param edgeAggregators   aggregate functions for super edges
   */
  public BuildEdgeGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> edgeAggregators) {
    super(groupPropertyKeys, useLabel, edgeAggregators);
    this.reuseEdgeGroupItem = new EdgeGroupItem();
    if (!doAggregate()) {
      this.reuseEdgeGroupItem.setAggregateValues(
        PropertyValueList.createEmptyList());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeGroupItem map(E edge) throws Exception {
    reuseEdgeGroupItem.setSourceId(edge.getSourceId());
    reuseEdgeGroupItem.setTargetId(edge.getTargetId());
    reuseEdgeGroupItem.setGroupLabel(getLabel(edge));
    reuseEdgeGroupItem.setGroupingValues(getGroupProperties(edge));
    if (doAggregate()) {
      reuseEdgeGroupItem.setAggregateValues(getAggregateValues(edge));
    }
    return reuseEdgeGroupItem;
  }
}
